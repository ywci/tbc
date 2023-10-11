#include "client.h"
#include "publisher.h"
#include "requester.h"
#include "evaluator.h"
#include "subscriber.h"

#ifdef EVAL_ECHO
#define CLI_EVAL
#endif

#ifdef CLI_EVAL
#include "verify.h"
#include "evaluator.h"

struct {
    int cnt;
    bool init;
    timeval_t start;
    uint64_t latency;
} client_status;

void client_connect_evaluator()
{
    rep_t rep;
    req_t req;
    char addr[ADDR_SIZE];
    struct in_addr src = get_addr();
    int evaluator = get_evaluator(get_hid());

    memcpy(&req, &src, sizeof(req_t));
    tcpaddr(addr, nodes[evaluator], evaluator_port);
    if (request(addr, &req, &rep))
        log_err("cannot connect to evaluator");
    else
        log_func("connect to %s", nodes[evaluator]);
}


void *client_start_listener(void *arg)
{
    int ret;
    void *socket;
    void *context;
    char addr[ADDR_SIZE];
#ifdef HIGH_WATER_MARK
    int hwm = HIGH_WATER_MARK;
#endif
    context = zmq_ctx_new();
    socket = zmq_socket(context, ZMQ_PULL);
#ifdef HIGH_WATER_MARK
    zmq_setsockopt(socket, ZMQ_RCVHWM, &hwm, sizeof(hwm));
#endif
    tcpaddr(addr, inet_ntoa(get_addr()), listener_port);
    ret = zmq_bind(socket, addr);
    if (ret) {
        log_err("failed to start listener, addr=%s, port=%d", inet_ntoa(get_addr()), listener_port);
        return NULL;
    }
    while (true) {
        timeval_t now;
        zmsg_t *msg = zmsg_recv(socket);
        hdr_t *hdr = get_hdr(msg);

        get_time(now);
        if ((hdr->cnt & EVAL_SMPL) == EVAL_SMPL) {
            client_status.cnt++;
            client_status.latency += time_diff(&hdr->t, &now);
        }
        if (hdr->cnt == eval_intv - 1) {
            float cps; // (cmd/sec)
            float latency;

            cps = (hdr->cnt + 1) / (time_diff(&client_status.start, &now) / 1000000.0);
            latency = client_status.latency / (float)client_status.cnt / 1000000.0;
            show_result("latency=%f (sec), cps=%f, sampled_requests=%d, time=%lu (usec, total latency of all sampled requests)\n", latency, cps, client_status.cnt, (unsigned long)client_status.latency);
#ifdef EVAL_THROUGHPUT
#ifdef EVAL_LATENCY
            log_file("latency=%f, cps=%f", latency, cps);
#else
            log_file("cps=%f", cps);
#endif
#elif defined(EVAL_LATENCY)
            log_file("latency=%f", latency);
#endif
        }
        zmsg_destroy(&msg);
    }
}


void client_create_listener()
{
    pthread_t thread;
    pthread_attr_t attr;

    log_file_remove();
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM);
    pthread_create(&thread, &attr, client_start_listener, NULL);
    pthread_attr_destroy(&attr);
}
#endif


static inline zmsg_t *client_add_timestamp(zmsg_t *msg)
{
    timeval_t curr;
    zframe_t *frame;
    static bool init = false;
    static timestamp_t timestamp;
    timestamp_t *ptimestamp = &timestamp;
#ifdef COUNT
    static uint32_t count = 0;
    static uint32_t sec = 0;
#else
    static timeval_t t;
#endif
    if (!init) {
        timestamp.hid = get_hid();
        init = true;
    }
#ifdef COUNT
    get_time(curr);
    sec = curr.tv_sec;
    count++;
    timestamp_set(ptimestamp, sec, count);
#else
    do {
        get_time(curr);
    } while ((curr.tv_sec == t.tv_sec) && (curr.tv_usec == t.tv_usec));
    t = curr;
    timestamp_set(ptimestamp, curr);
#endif
    frame = zframe_new(ptimestamp, sizeof(timestamp_t));
    zmsg_prepend(msg, &frame);
#ifdef CLI_EVAL
    if (!client_status.init) {
        get_time(client_status.start);
        client_status.init = true;
    }
#endif
    return msg;
}


zmsg_t *client_set_msg(zmsg_t *msg)
{
    return client_add_timestamp(msg);
}


int client_create()
{
    pub_arg_t *arg;
    pthread_t thread;
    pthread_attr_t attr;

    arg = (pub_arg_t *)calloc(1, sizeof(pub_arg_t));
    if (!arg) {
        log_err("no memory");
        return -ENOMEM;
    }
    strcpy(arg->src, TBC_ADDR);
    if (MULTICAST == MULTICAST_PUB)
        strcpy(arg->addr, CLIENT_ADDR);
    else if (MULTICAST == MULTICAST_SUB)
        tcpaddr(arg->addr, inet_ntoa(get_addr()), client_port);
    else if (MULTICAST == MULTICAST_PGM)
        pgmaddr(arg->addr, inet_ntoa(get_addr()), client_port);
    else if (MULTICAST == MULTICAST_EPGM)
        epgmaddr(arg->addr, inet_ntoa(get_addr()), client_port);
    for (int i = 0; i < nr_nodes; i++)
        tcpaddr(arg->dest[i], nodes[i], generator_port);
    arg->total = nr_nodes;
    arg->callback = client_set_msg;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM);
    pthread_create(&thread, &attr, publisher_start, arg);
    pthread_attr_destroy(&attr);
#ifdef CLI_EVAL
    client_status.cnt = 0;
    client_status.init = false;
    client_create_listener();
    client_connect_evaluator();
#endif
    return 0;
}
