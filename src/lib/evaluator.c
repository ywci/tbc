#include "evaluator.h"
#include "verify.h"

#ifdef EVAL_ECHO
#include "responder.h"
typedef rbtree_t eval_tree_t;
typedef rbtree_node_t eval_node_t;
#define eval_tree_create rb_tree_new
#define eval_node_lookup rb_tree_find
#define eval_node_insert rb_tree_insert

typedef struct {
    int cnt;
    hid_t hid;
    void *desc;
    eval_node_t node;
} eval_record_t;
#endif

struct {
#ifdef EVAL_ECHO
    eval_tree_t tree;
#else
    int cnt;
    hid_t hid;
    bool init;
    int updates;
    timeval_t start;
    uint64_t latency;
#endif
} eval_status;

#ifdef EVAL_ECHO
int eval_node_compare(const void *id1, const void *id2)
{
    assert(id1 && id2);
    hid_t v1 = *(hid_t *)id1;
    hid_t v2 = *(hid_t *)id2;

    if (v1 > v2)
        return 1;
    else if (v1 < v2)
        return -1;
    else
        return 0;
}


inline eval_record_t *eval_lookup(eval_tree_t *tree, void *id)
{
    eval_node_t *node = NULL;

    if (!eval_node_lookup(tree, id, &node))
        return tree_entry(node, eval_record_t, node);
    else
        return NULL;
}


void eval_response(char *buf, size_t size)
{
    hdr_t *hdr = (hdr_t *)buf;
    assert(size >= sizeof(hdr_t));

    if (node_id == get_evaluator(hdr->hid)) {
        eval_record_t *rec = eval_lookup(&eval_status.tree, (void *)&hdr->hid);

        assert(hdr->cnt == rec->cnt);
        if (((rec->cnt & EVAL_SMPL) == EVAL_SMPL) || (rec->cnt == eval_intv - 1)) {
            zmsg_t *msg = zmsg_new();
            zframe_t *frame = zframe_new(buf, sizeof(hdr_t));

            zmsg_append(msg, &frame);
            zmsg_send(&msg, rec->desc);
        }
        rec->cnt++;
    }
}


rep_t eval_responder(req_t req)
{
    hid_t hid;
    void *context;
    eval_record_t *rec;
    struct in_addr addr;
    char dest[ADDR_SIZE];
#ifdef HIGH_WATER_MARK
    int hwm = HIGH_WATER_MARK;
#endif
    rec = calloc(1, sizeof(eval_record_t));
    assert(rec);
    context = zmq_ctx_new();
    memcpy(&addr, &req, sizeof(struct in_addr));
    tcpaddr(dest, inet_ntoa(addr), listener_port);
    rec->cnt = 0;
    rec->hid = addr2hid(addr);
    rec->desc = zmq_socket(context, ZMQ_PUSH);
#ifdef HIGH_WATER_MARK
    zmq_setsockopt(rec->desc, ZMQ_SNDHWM, &hwm, sizeof(hwm));
#endif
    if (zmq_connect(rec->desc, dest)) {
        free(rec);
        log_err("failed to connect");
        return -EINVAL;
    }
    if (eval_node_insert(&eval_status.tree, &rec->hid, &rec->node)) {
        log_err("failed to insert");
        assert(0);
    }
    log_func("addr=%s", inet_ntoa(addr));
    return 0;
}


void eval_create_responder()
{
    pthread_t thread;
    pthread_attr_t attr;
    responder_arg_t *arg;

    arg = (responder_arg_t *)calloc(1, sizeof(responder_arg_t));
    assert(arg);
    arg->responder = eval_responder;
    tcpaddr(arg->addr, inet_ntoa(get_addr()), evaluator_port);
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM);
    pthread_create(&thread, &attr, responder_start, arg);
    pthread_attr_destroy(&attr);
}
#else
void eval_handle(char *buf, size_t size)
{
    hdr_t *hdr = (hdr_t *)buf;
    assert(size >= sizeof(hdr_t));
#ifdef VERIFY
    verify_output(hdr);
#endif
    if (!eval_status.init) {
        get_time(eval_status.start);
        eval_status.init = true;
    }
#ifdef EVAL_LATENCY
    if ((hdr->hid == eval_status.hid) && ((eval_status.updates & EVAL_SMPL) == EVAL_SMPL)) {
        timeval_t now;

        get_time(now);
        eval_status.latency += time_diff(&hdr->t, &now);
        eval_status.cnt++;
    }
#endif
    if (eval_status.updates == eval_intv - 1) {
        float cps; // commands per second
        timeval_t now;

        get_time(now);
        cps = eval_intv / (time_diff(&eval_status.start, &now) / 1000000.0);
        if (eval_status.latency) {
            float latency = eval_status.latency / (float)eval_status.cnt / 1000000.0;
            // The reported latency may not be highly accurate due to the potential buffering of messages before their actual transmission by the clients.
            show_result("latency=%fsec, cps=%f, requests=%d\n", latency, cps, eval_status.cnt);
        } else
            show_result("cps=%f\n", cps);
        eval_status.init = false;
        eval_status.updates = 0;
#ifdef EVAL_LATENCY
        eval_status.latency = 0;
        eval_status.cnt = 0;
#endif
#ifdef EVAL_THROUGHPUT
#ifdef EVAL_LATENCY
        if (eval_status.latency) {
            float latency = eval_status.latency / (float)eval_status.cnt / 1000000.0;
            
            log_file("latency=%f, cps=%f", latency, cps);
        } else
            log_file("cps=%f", cps);
#else
        log_file("cps=%f", cps);
#endif
#elif defined(EVAL_LATENCY)
        log_file("latency=%f", latency);
#endif
    } else
        eval_status.updates++;
}
#endif


void evaluate(char *buf, size_t size)
{
#ifdef EVAL_ECHO
    eval_response(buf, size);
#else
    eval_handle(buf, size);
#endif
}


void eval_create()
{
    log_file_remove();
#ifdef VERIFY
    verify_init();
#endif
#ifdef EVAL_ECHO
    if (!eval_tree_create(&eval_status.tree, eval_node_compare))
        eval_create_responder();
    else
        log_err("failed to create");
#else
    eval_status.cnt = 0;
    eval_status.latency = 0;
    eval_status.updates = 0;
    eval_status.init = false;
    eval_status.hid = get_hid();
#endif
}
