#include <tbc.h>
#include "generator.h"
#include "collector.h"
#include "publisher.h"
#include "subscriber.h"
#include "tracker.h"

// #define COLL_NOWAIT
#define COLL_WAITTIME  1000000      // nsec

typedef enum {
    STATE_IDLE = 0,
    STATE_SUSPECT,
    STATE_RESUME,
    STATE_SYNC,
    NR_STATES,
} coll_state_t;

typedef struct {
    seq_t start;
    seq_t end;
    int id;
} coll_range_t;

typedef struct {
    int src;
    bitmap_t suspect;
    session_t session;
    coll_state_t state;
    bitmap_t recoverable;
    seq_t seq[NODE_MAX];
} coll_req_t;

struct {
    bitmap_t suspect;
    coll_state_t state;
    seq_t seq[NODE_MAX];
    pthread_mutex_t lock;
    bitmap_t recoverable;
    bitmap_t providers[NODE_MAX];
    bitmap_t members[NR_STATES][NODE_MAX];
} collector_status;

#define STATE_UNKNOWN NR_STATES

#define coll_state2str(state) collector_states[state]
#define coll_state_current() (collector_status.state)
#define coll_state_next() (collector_status.state + 1)
#define coll_set_state(s) (collector_status.state = s)
#define coll_state() coll_state2str(collector_status.state)

char collector_states[NR_STATES + 1][32] = {"[idle]",  "[suspect]", "[resume]", "[sync]"};

void collector_lock()
{
    pthread_mutex_lock(&collector_status.lock);
}


void collector_unlock()
{
    pthread_mutex_unlock(&collector_status.lock);
}


void collector_connect()
{
    for (int i = 0; i < nr_nodes; i++) {
        if (i != node_id) {
            sub_arg_t *arg;
            pthread_t thread;
            pthread_attr_t attr;

            arg = (sub_arg_t *)malloc(sizeof(sub_arg_t));
            if (!arg) {
                log_err("no memory");
                return;
            }
            tcpaddr(arg->src, nodes[i], collector_port);
            strcpy(arg->dest, COLLECTOR_BACKEND);
            pthread_attr_init(&attr);
            pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
            pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM);
            pthread_create(&thread, &attr, subscriber_start, arg);
            pthread_attr_destroy(&attr);
        }
    }
}


void collector_reset()
{
    for (int i = 0; i < NR_STATES; i++)
        for (int j = 0; j < NODE_MAX; j++)
            collector_status.members[i][j] = 0;
    for (int i = 0; i < nr_nodes; i++) {
        collector_status.seq[i] = 0;
        collector_status.providers[i] = 0;
    }
    coll_set_state(STATE_IDLE);
}


void collector_send_request(coll_req_t *req)
{
    void *context = zmq_ctx_new();
    void *socket = zmq_socket(context, ZMQ_PUSH);
    size_t size = sizeof(coll_req_t) - (NODE_MAX - nr_nodes) * sizeof(seq_t);

    if (!zmq_connect(socket, COLLECTOR_FRONTEND)) {
        zmq_send(socket, req, size, 0);
        zmq_close(socket);
        zmq_ctx_destroy(context);
    } else
        log_err("failed to send request");
}


void collector_send_timestamps(int id, timestamp_t *timestamps, seq_t start, seq_t end)
{
    void *context = zmq_ctx_new();
    void *socket = zmq_socket(context, ZMQ_PUSH);

    if (!zmq_connect(socket, COLLECTOR_FRONTEND)) {
        coll_req_t r;
        coll_range_t range;
        zframe_t *frame = NULL;
        zmsg_t *msg = zmsg_new();
        size_t sz = (end - start + 1) * sizeof(timestamp_t);

        memset(&r, 0, sizeof(coll_req_t));
        r.src = node_id;
        r.state = STATE_SYNC;
        r.session = get_session(node_id);
        r.suspect = collector_status.suspect;
        frame = zframe_new(&r, sizeof(coll_req_t));
        zmsg_append(msg, &frame);
        
        range.id = id;
        range.end = end;
        range.start = start;
        frame = zframe_new(&range, sizeof(coll_range_t));
        zmsg_append(msg, &frame);
        
        frame = zframe_new(timestamps, sz);
        zmsg_append(msg, &frame);
        zmsg_send(&msg, socket);
        zmq_close(socket);
        zmq_ctx_destroy(context);
        crash_debug("send timestamps of node %d (the range of seq is (%d, %d))", id, start, end);
    } else
        log_err("failed to send timestamps");
}


bool collector_can_ignore(coll_req_t *req)
{
    int src = req->src;
    bool ignore = false;
    coll_state_t state = req->state;

    if (req->session != get_session(src)) {
        crash_debug("invalid session %d (exprected=%d)", req->session, get_session(src));
        return true;
    }
    switch (state) {
    case STATE_SYNC:
        if (coll_state_current() != STATE_SUSPECT) {
            ignore = true;
            break;
        }
    case STATE_RESUME:
    case STATE_SUSPECT:
        if (node_mask[src] & collector_status.suspect)
            ignore = true;
        break;
    default:
        break;
    }
    if (ignore)
        crash_debug("ignore a request from node %d, state=%s, current=%s", src, coll_state2str(state), coll_state());
    return ignore;
}


bool collector_need_abort(coll_req_t *req)
{
    int ret = false;
    int src = req->src;
    int state = req->state;
    int current = coll_state_current();

    if (collector_status.suspect && (collector_status.suspect != req->suspect)) {
        crash_details("found mismatch of suspect, state=%s, current=%s node=%d", coll_state2str(state), coll_state(), req->src);
        ret = true;
        goto out;
    }
    for (int i = 0; i < nr_nodes; i++) {
        bitmap_t suspect = collector_status.members[state][i];

        if (!(collector_status.suspect & node_mask[i]) && suspect && (suspect != req->suspect)) {
            crash_details("found mismatch of suspect, state=%s, current=%s <node%d>", coll_state2str(state), coll_state(), req->src);
            ret = true;
            goto out;
        }
    }
    switch(state) {
    case STATE_RESUME:
        if ((current != STATE_IDLE) && (current != STATE_SUSPECT) && (current != state)) {
            crash_details("invalid state %s, current=%s <node%d>", coll_state2str(state), coll_state(), src);
            ret = true;
        }
        break;
    case STATE_SUSPECT:
        if ((current != STATE_IDLE) && (current != STATE_RESUME) && (current != state)) {
            crash_details("invalid state %s, current=%s <node%d>", coll_state2str(state), coll_state(), src);
            ret = true;
        }
        break;
    default:
        break;
    }
out:
    return ret;
}


bool collector_check_members(coll_state_t state)
{
    bitmap_t current = node_mask[node_id];
    bitmap_t available = available_nodes & (~collector_status.suspect);

    for (int i = 0; i < nr_nodes; i++)
        if (collector_status.members[state][i] == collector_status.suspect)
            current |= node_mask[i];
    crash_bitmap_details(coll_state2str(state), "members", current);
    return (available & current) == available;
}


bool collector_check_state(coll_req_t *req)
{
    coll_state_t state = req ? req->state : collector_status.state;

    if (req)
        collector_status.members[req->state][req->src] = req->suspect;
    return (state == collector_status.state) && collector_check_members(state);
}


void collector_check_suspect(coll_req_t *req)
{
    if (collector_check_state(req)) {
        coll_req_t r;
        bitmap_t available = available_nodes & (~collector_status.suspect);

        for (int i = 0; i < nr_nodes; i++) {
            bitmap_t providers = collector_status.providers[i];

            if (providers && ((providers & available) != available)) {
                for (int j = 0; j < nr_nodes; j++) {
                    if (providers & node_mask[j]) {
                        if (j == node_id) {
                            seq_t seq;
                            seq_t seq_start;
                            timestamp_t *timestamps = NULL;
                            seq_t seq_end = collector_status.seq[i];

                            if (!get_seq_end(i, &seq)) {
                                log_err("failed to get seq end");
                                assert(0);
                            }
                            if (!get_seq_start(i, &seq_start)) {
                                log_err("failed to get seq start");
                                assert(0);
                            }
                            assert((seq == seq_end) && (seq_start <= seq_end));
                            timestamps = (timestamp_t *)malloc((seq_end - seq_start + 1) * sizeof(timestamp_t));
                            get_timestamps(i, timestamps, seq_start, seq_end);
                            collector_send_timestamps(i, timestamps, seq_start, seq_end);
                            free(timestamps);
                        }
                        break;
                    }
                }
            }
        }
        memset(&r, 0, sizeof(coll_req_t));
        r.src = node_id;
        r.state = STATE_RESUME;
        r.session = get_session(node_id);
        r.suspect = collector_status.suspect;
        collector_send_request(&r);
        coll_set_state(STATE_RESUME);
        crash_debug("resume (session=%d)", get_session(node_id));
    }
}


void collector_check_resume(coll_req_t *req)
{
    if (collector_check_state(req)) {
        bitmap_t suspect = collector_status.suspect;

        for (int i = 0; i < nr_nodes; i++) {
            if ((suspect & node_mask[i]) && (tracker_get_liveness(i) == SUSPECT)) {
                session_t session = get_session(i);

                set_session(i, session + 1);
                tracker_set_liveness(i, STOP);
            }
        }
        available_nodes = available_nodes & ~suspect;
        // collector_status.suspect = 0;
        for (int i = 0; i < nr_nodes; i++) {
            if (available_nodes & node_mask[i])
                alive_node[i] = true;
            else
                alive_node[i] = false;
        }
        generator_resume();
        collector_reset();
        crash_show_bitmap("collector resumes", "available_nodes", available_nodes);
        crash_debug("collector resumes (session=%d)", get_session(node_id));
        debug_quiet_after_resume();
        debug_crash_after_resume();
    }
}


void collector_do_fault()
{
    if (collector_status.suspect) {
        coll_req_t r;
        coll_state_t state = coll_state_current();

        assert(STATE_IDLE == state);
        generator_suspend();
        memset(&r, 0, sizeof(coll_req_t));
        r.src = node_id;
        r.state = STATE_SUSPECT;
        r.session = get_session(node_id);
        r.suspect = collector_status.suspect;
        for (int i = 0; i < nr_nodes; i++) {
            if (collector_status.suspect & node_mask[i]) {
                seq_t seq;

                tracker_suspect(i);
                if (get_seq_end(i, &seq)) {
                    assert(seq > 0);
                    r.seq[i] = seq;
                }
            }
        }
        coll_set_state(STATE_SUSPECT);
        collector_send_request(&r);
        collector_check_suspect(NULL);
        collector_check_resume(NULL);
        show_suspect(collector_status.suspect, r.seq);
    }
}


void collector_fault(int id)
{
    collector_lock();
    if (!(collector_status.suspect & node_mask[id])) {
        collector_status.suspect |= node_mask[id];
        if (collector_status.state != STATE_IDLE)
            collector_reset();
        collector_do_fault();
    }
    collector_unlock();
}


void collector_abort(coll_req_t *req)
{
    collector_reset();
    collector_do_fault();
    crash_details("abort <node%d>", req->src);
}


void collector_suspect(coll_req_t *req)
{
    for (int i = 0; i < nr_nodes; i++) {
        seq_t seq = req->seq[i];

        if (seq > 0) {
            if (seq > collector_status.seq[i]) {
                collector_status.seq[i] = seq;
                collector_status.providers[i] = node_mask[req->src];
            } else if (seq == collector_status.seq[i])
                collector_status.providers[i] |= node_mask[req->src];
        }
    }
    show_providers(collector_status.seq, collector_status.suspect, collector_status.providers);
    collector_check_suspect(req);
}


void collector_sync(coll_req_t *req, coll_range_t *range, timestamp_t *timestamps, zmsg_t *msg)
{
    seq_t end;
    seq_t start;
    int id = range->id;
    timestamp_t *p = timestamps;

    assert(range);
    assert(timestamps);
    if (get_seq_start(id, &start))
        assert(start >= range->start);
    if (get_seq_end(id, &end))
        assert(end <= range->end);
    add_timestamps(id, timestamps, range->end - range->start + 1, msg);
    crash_debug("the node %d provides the timestamps of the node %d (the range of seq is (%d, %d))", req->src, id, range->start, range->end);
}


void collector_resume(coll_req_t *req)
{
    collector_check_resume(req);
}


void collector_wait()
{
#ifndef COLL_NOWAIT
    usleep(COLL_WAITTIME);
#endif
}


void *collector_handle(void *ptr)
{
    int ret;
    void *context = zmq_ctx_new();
    void *socket = zmq_socket(context, ZMQ_PULL);

    ret = zmq_bind(socket, COLLECTOR_BACKEND);
    if (ret) {
        log_err("failed to bind to %s", COLLECTOR_BACKEND);
        goto out;
    }
    while (true) {
        coll_range_t *range = NULL;
        timestamp_t *timestamps = NULL;
        zmsg_t *msg = zmsg_recv(socket);
        zframe_t *frame = zmsg_first(msg);
        coll_req_t *req = (coll_req_t *)zframe_data(frame);
        coll_state_t state = req->state;

        frame = zmsg_next(msg);
        if (frame) {
            range = (coll_range_t *)zframe_data(frame);
            frame = zmsg_next(msg);
            if (frame)
                timestamps = (timestamp_t *)zframe_data(frame);
        }
        collector_lock();
        if (!collector_can_ignore(req)) {
            if (collector_need_abort(req)) {
                collector_abort(req);
                collector_wait();
            } else {
                switch(state) {
                case STATE_SUSPECT:
                    collector_suspect(req);
                    break;
                case STATE_SYNC:
                    collector_sync(req, range, timestamps, msg);
                    msg = NULL;
                    break;
                case STATE_RESUME:
                    collector_resume(req);
                    break;
                default:
                    assert(0);
                }
            }
        }
        collector_unlock();
        if (msg)
            zmsg_destroy(&msg);
    }
out:
    zmq_close(socket);
    zmq_ctx_destroy(context);
    return NULL;
}


void collector_init()
{
    collector_status.suspect = 0;
    collector_status.recoverable = 0;
    collector_status.state = STATE_IDLE;
    pthread_mutex_init(&collector_status.lock, NULL);
    for (int i = 0; i < NR_STATES; i++)
        for (int j = 0; j < NODE_MAX; j++)
            collector_status.members[i][j] = 0;
    for (int i = 0; i < NODE_MAX; i++) {
        collector_status.seq[i] = 0;
        collector_status.providers[i] = 0;
    }
}


void collector_create()
{
    pub_arg_t *arg;
    pthread_t thread;
    pthread_attr_t attr;

    collector_init();
    arg = (pub_arg_t *)calloc(1, sizeof(pub_arg_t));
    if (!arg) {
        log_err("no memory");
        return;
    }
    arg->type = MULTICAST_PUB;
    strcpy(arg->src, COLLECTOR_FRONTEND);
    tcpaddr(arg->addr, inet_ntoa(get_addr()), collector_port);

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM);
    pthread_create(&thread, &attr, publisher_start, arg);
    pthread_attr_destroy(&attr);

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM);
    pthread_create(&thread, &attr, collector_handle, NULL);
    pthread_attr_destroy(&attr);
    collector_connect();
}
