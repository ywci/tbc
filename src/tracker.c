#include "ev.h"
#include "queue.h"
#include "batch.h"
#include "record.h"
#include "handler.h"
#include "generator.h"
#include "responder.h"
#include "requester.h"
#include "timestamp.h"
#include "evaluator.h"
#include "tracker.h"

#define TRACKER_CHECK_INTV 5000000 // nsec
#define TRACKER_QUEUE_CHECKER
#define TRACKER_IGNORE

typedef struct tracker_arg {
    int id;
    int type;
    char addr[ADDR_SIZE];
} tracker_arg_t;

typedef struct tracker_entry {
    record_t *record;
    struct list_head list;
} tracker_entry_t;

struct {
    bool busy;
    ev_t ev_deliver;
    pthread_mutex_t mutex;
    ev_t ev_live[NODE_MAX];
    struct list_head output;
    pthread_mutex_t deliver_lock;
    liveness_t liveness[NODE_MAX];
    pthread_mutex_t locks[NODE_MAX];
    struct list_head checked[NODE_MAX];
    struct list_head input[NODE_MAX];
    struct list_head req_list[NODE_MAX];
    pthread_mutex_t recv_locks[NODE_MAX];
    struct list_head candidates[NODE_MAX];
} tracker_status;

#define tracker_list_head_init(phead, pnext) do { \
    phead->next = pnext; \
    phead->prev = pnext; \
    pnext->next = phead; \
    pnext->prev = phead; \
} while (0)

#define tracker_list_del(p) do { \
    list_del(p); \
    set_empty(p); \
} while (0)

#ifdef TRACKER_IGNORE
#define tracker_ignore(rec) do { \
    if (!(rec)->ignore \
        && ((rec)->perceived < majority) \
        && (((rec)->receivers & available_nodes) == available_nodes)) \
        (rec)->ignore = true; \
} while (0)
#else
#define tracker_ignore(...) do {} while (0)
#endif

#define tracker_lock(id) pthread_mutex_lock(&tracker_status.locks[id])
#define tracker_unlock(id) pthread_mutex_unlock(&tracker_status.locks[id])

#define tracker_mutex_lock() pthread_mutex_lock(&tracker_status.mutex)
#define tracker_mutex_unlock() pthread_mutex_unlock(&tracker_status.mutex)

#define tracker_recv_lock(id) pthread_mutex_lock(&tracker_status.recv_locks[id])
#define tracker_recv_unlock(id) pthread_mutex_unlock(&tracker_status.recv_locks[id])

#define tracker_deliver_lock() pthread_mutex_lock(&tracker_status.deliver_lock)
#define tracker_deliver_unlock() pthread_mutex_unlock(&tracker_status.deliver_lock)

#define tracker_list_add assert_list_add
#define tracker_list_add_tail assert_list_add_tail
#define tracker_can_deliver(rec) (((rec)->perceived >= majority) && !is_delivered(rec))

uint64_t tracker_deliver_cnt = 0;

void tracker_deliver(record_t *record)
{
    bool wakeup = false;

    tracker_status.busy = true;
    tracker_deliver_lock();
    if (!is_delivered(record)) {
        record_deliver(record);
        if (list_empty(&tracker_status.output))
            wakeup = true;
        tracker_list_add_tail(&record->output, &tracker_status.output);
        tracker_deliver_cnt++;
        show_deliver(record, tracker_deliver_cnt);
    }
    tracker_deliver_unlock();
    if (wakeup)
        ev_set(&tracker_status.ev_deliver);
}


static inline void tracker_check_queue(int id, struct list_head *head, struct list_head *pos, bool ignore)
{
    assert(head && is_valid(head));
    if (pos) {
        bool empty = false;
        record_t *rec = NULL;
        struct list_head *checked = NULL;
        struct list_head *candidates = &tracker_status.candidates[id];

        while (pos != candidates) {
            rec = list_entry(pos, record_t, cand[id]);
            checked = &rec->checked[id];
            if (!is_delivered(rec)) {
                empty = is_empty(checked);
                if (!ignore)
                    tracker_ignore(rec);
                if (empty && rec->ignore) {
                    tracker_list_add(checked, head);
                    if (!rec->prev[id] && !rec->count[id]) {
                        rec->count[id] = true;
                        rec->perceived++;
                        if (tracker_can_deliver(rec)) {
                            tracker_deliver(rec);
                            return;
                        }
                    }
                } else
                    break;
            }
            if (is_valid(checked))
                head = checked;
            pos = pos->next;
        }
        if ((pos != candidates) && empty && !rec->count[id] && !rec->prev[id]) {
            rec->count[id] = true;
            rec->perceived++;
            if (tracker_can_deliver(rec))
                tracker_deliver(rec);
        }
    }
}


static inline void tracker_check(record_t *record)
{
    if (!record->ignore && ((record->receivers & available_nodes) == available_nodes)) {
        for (int id = 0; id < nr_nodes; id++) {
            struct list_head *cand = &record->cand[id];

            if (is_valid(cand)) {
                struct list_head *head = NULL;
                struct list_head *prev = cand->prev;
                struct list_head *candidates = &tracker_status.candidates[id];

                if (prev != candidates) {
                    record_t *rec = list_entry(prev, record_t, cand[id]);

                    head = &rec->checked[id];
                    if (is_empty(head)) {
                        head = NULL;
                        if (is_delivered(rec)) {
                            prev = prev->prev;
                            while (prev != candidates) {
                                rec = list_entry(prev, record_t, cand[id]);
                                if (is_delivered(rec))
                                    prev = prev->prev;
                                else {
                                    if (is_valid(&rec->checked[id]))
                                        head = &rec->checked[id];
                                    break;
                                }
                            }
                            if (prev == candidates)
                                head = &tracker_status.checked[id];
                        }
                    }
                } else
                    head = &tracker_status.checked[id];

                if (head) {
                    struct list_head *pos = cand->next;
                    struct list_head *checked = &record->checked[id];

                    assert(is_empty(checked));
                    tracker_list_add(checked, head);
                    tracker_check_queue(id, checked, pos, true);
                }
            }
        }
        assert(!record->ignore);
        record->ignore = true;
    }
}


void tracker_check_receivers(int id, record_t *record)
{
    bool valid = true;
    struct list_head *last;

    if (is_empty(&record->cand[id])) {
        struct list_head *input = &record->input[id];

        assert(is_empty(input));
        tracker_list_add_tail(input, &tracker_status.input[id]);
        tracker_wakeup();
        return;
    }
    tracker_mutex_lock();
    if (is_valid(&record->input[id]))
        record->receivers |= node_mask[id];
    last = record->cand[id].prev;
    assert(last);
    if (last != &tracker_status.candidates[id]) {
        record_t *rec = list_entry(last, record_t, cand[id]);

        valid = rec->count[id] || is_valid(&rec->checked[id]);
        if (!valid)
            show_blocker(id, rec, record);
    }
    if (valid && !record->count[id]) {
        record->count[id] = true;
        record->perceived++;
        if (record->perceived >= majority)
            if (!is_delivered(record))
                tracker_deliver(record);
    }
    if (record->perceived < majority)
        tracker_check(record);
    tracker_mutex_unlock();
    show_perceived(id, record);
}


inline bool tracker_check_next(int id, record_t *rec_next, record_t *rec_prev)
{
    bool valid = rec_prev ? (is_valid(&rec_prev->checked[id]) || rec_prev->count[id]) : true;

    tracker_ignore(rec_next);
    if (valid && !rec_next->count[id] && !rec_next->prev[id]) {
        rec_next->count[id] = true;
        rec_next->perceived++;
        if (tracker_can_deliver(rec_next))
            tracker_deliver(rec_next);
        return true;
    } else
        return false;
}


void tracker_delete_entry(int id, record_t *record)
{
    record_t *rec_next = NULL;
    record_t *rec_prev = NULL;
    record_t *pprev = record->prev[id];
    struct list_head *req = &record->req[id];
    struct list_head *cand = &record->cand[id];
    struct list_head *next = &record->next[id];
    struct list_head *input = &record->input[id];
    struct list_head *checked = &record->checked[id];
    struct list_head *head = &tracker_status.checked[id];
    struct list_head *candidates = &tracker_status.candidates[id];

    show_dequeue(id, record->timestamp);
    if (pprev) {
        assert(timestamp_compare(pprev->timestamp, record->timestamp) < 0);
        tracker_list_del(&record->link[id]);
    }
    if (is_valid(next) && !list_empty(next)) {
        struct list_head *i;
        struct list_head *j;
        record_t *prev = NULL;

        for (i = next->next, j = i->next; i != next; i = j, j = j->next) {
            record_t *rec = list_entry(i, record_t, link[id]);

            prev = queue_update_prev(id, record, rec);
            if (pprev && !prev) {
                show_prev(id, rec, pprev, record);
                log_err("failed to find prev item");
            }
            if (prev) {
                struct list_head *prev_next = &prev->next[id];

                if (is_empty(prev_next))
                    tracker_list_head_init(prev_next, i);
                else
                    tracker_list_add_tail(i, prev_next);
                rec->prev[id] = prev;
                show_prev(id, rec, prev, record);
            } else {
                set_empty(i);
                rec->prev[id] = NULL;
                show_prev(id, rec, NULL, record);
                tracker_check_receivers(id, rec);
            }
        }
    }
    if (is_valid(cand)) {
        if (cand->next != candidates)
            rec_next = list_entry(cand->next, record_t, cand[id]);
        if (cand->prev != candidates)
            rec_prev = list_entry(cand->prev, record_t, cand[id]);
    }
    tracker_mutex_lock();
    if (rec_prev)
        head = &rec_prev->checked[id];
    if (is_valid(checked))
        tracker_list_del(checked);
    if (rec_next) {
        tracker_check_next(id, rec_next, rec_prev);
        if (is_valid(head))
            tracker_check_queue(id, head, &rec_next->cand[id], true);
    }
    if (is_valid(cand))
        tracker_list_del(cand);
    tracker_mutex_unlock();
    queue_pop(id, record);
    if (is_valid(input))
        tracker_list_del(input);
    if (is_valid(req))
        tracker_list_del(req);
}


void tracker_update_queue(int id, record_t *record)
{
    bool earliest = false;

    show_enqueue(id, record->timestamp);
    queue_push(id, record, &earliest);
    tracker_list_add_tail(&record->req[id], &tracker_status.req_list[id]);
    if (earliest) {
        tracker_list_add_tail(&record->input[id],  &tracker_status.input[id]);
        tracker_wakeup();
    }
}


static inline record_t *tracker_do_check_output()
{
    record_t *record = NULL;

    tracker_deliver_lock();
    if (!list_empty(&tracker_status.output)) {
        struct list_head *head = tracker_status.output.next;

        record = list_entry(head, record_t, output);
        tracker_list_del(head);
    }
    tracker_deliver_unlock();
    return record;
}


bool tracker_check_output()
{
    record_t *rec = tracker_do_check_output();

    if (rec) {
        hid_t hid;
        timeval_t t;
        zmsg_t *msg = rec->msg;
        zframe_t *frame = zmsg_last(msg);
        size_t size = zframe_size(frame);
        char *buf = (char *)zframe_data(frame);

        for (int i = 0; i < nr_nodes; i++) {
            tracker_lock(i);
            if (!is_empty(&rec->item_list[i]))
                tracker_delete_entry(i, rec);
            tracker_unlock(i);
        }
        handle(buf, size);
        record_release(rec);
        return true;
    } else
        return false;
}


bool tracker_check_input()
{
    bool ret = false;

    for (int id = 0; id < nr_nodes; id++) {
        struct list_head *i;
        struct list_head *j;
        bitmap_t mask = node_mask[id];
        struct list_head *checked = &tracker_status.checked[id];
        struct list_head *input = &tracker_status.input[id];
        struct list_head *req_list = &tracker_status.req_list[id];
        struct list_head *candidates = &tracker_status.candidates[id];

        tracker_lock(id);
        if (!list_empty(req_list)) {
            tracker_mutex_lock();
            for (i = req_list->next, j = i->next; i != req_list; i = j, j = j->next) {
                record_t *rec = list_entry(i, record_t, req[id]);

                if (is_empty(&rec->input[id]))
                    rec->receivers |= mask;

                if (!is_delivered(rec)) {
                    tracker_list_add_tail(&rec->cand[id], candidates);
                    if (rec->perceived < majority)
                        tracker_check(rec);
                }
                tracker_list_del(i);
            }
            tracker_mutex_unlock();
            ret = true;
        }
        if (!list_empty(input)) {
            for (i = input->next, j = i->next; i != input; i = j, j = j->next) {
                record_t *rec = list_entry(i, record_t, input[id]);

                if (!is_delivered(rec))
                    tracker_check_receivers(id, rec);
                tracker_list_del(i);
            }
            ret = true;
        }
        tracker_unlock(id);
    }
    return ret;
}


static inline void tracker_put(int id, record_t *record)
{
    track_enter();
    if (is_empty(&record->item_list[id]))
        tracker_update_queue(id, record);
    track_exit();
}


static inline void tracker_do_update(int id, timestamp_t *timestamp, zmsg_t *msg)
{
    track_enter();
    if (timestamp_check(timestamp)) {
        record_t *rec = record_find(id, timestamp, msg);

        if (rec) {
            if (!is_delivered(rec))
                tracker_put(id, rec);
            record_put(id, rec);
        }
    }
    track_exit();
}


void tracker_update(int id, timestamp_t *timestamp, zmsg_t *msg)
{
    tracker_lock(id);
    tracker_do_update(id, timestamp, msg);
    tracker_unlock(id);
}


void tracker_wakeup()
{
    ev_set(&tracker_status.ev_deliver);
}


int tracker_init()
{
    if (nr_nodes <= 0) {
        log_err("failed to initialize");
        return -1;
    }
    queue_init();
    tracker_status.busy = false;
    INIT_LIST_HEAD(&tracker_status.output);
    ev_init(&tracker_status.ev_deliver, DELIVER_TIMEOUT);
    for (int i = 0; i < NODE_MAX; i++) {
        INIT_LIST_HEAD(&tracker_status.checked[i]);
        INIT_LIST_HEAD(&tracker_status.input[i]);
        INIT_LIST_HEAD(&tracker_status.req_list[i]);
        INIT_LIST_HEAD(&tracker_status.candidates[i]);
        pthread_mutex_init(&tracker_status.locks[i], NULL);
        pthread_mutex_init(&tracker_status.recv_locks[i], NULL);
        ev_init(&tracker_status.ev_live[i], EV_NOTIMEOUT);
        tracker_status.liveness[i] = ALIVE;
    }
    pthread_mutex_init(&tracker_status.deliver_lock, NULL);
    pthread_mutex_init(&tracker_status.mutex, NULL);
    timestamp_init();
    record_init();
    return 0;
}


void tracker_suspect(int id)
{
    crash_details("start (id=%d)", id);
    tracker_recv_lock(id);
    tracker_status.liveness[id] = SUSPECT;
    tracker_recv_unlock(id);
    crash_details("finished (id=%d)", id);
}


void tracker_recover(int id)
{
    crash_details("start (id=%d)", id);
    tracker_recv_lock(id);
    tracker_status.liveness[id] = ALIVE;
    ev_set(&tracker_status.ev_live[id]);
    tracker_recv_unlock(id);
    crash_details("finished (id=%d)", id);
}


liveness_t tracker_get_liveness(int id)
{
    liveness_t ret;

    tracker_recv_lock(id);
    ret = tracker_status.liveness[id];
    tracker_recv_unlock(id);
    return ret;
}


void tracker_set_liveness(int id, liveness_t l)
{
    tracker_recv_lock(id);
    tracker_status.liveness[id] = l;
    tracker_recv_unlock(id);
}


void *tracker_do_connect(void *ptr)
{
    int id;
    int ret;
    void *socket;
    void *context;
    zmsg_t *msg = NULL;
    tracker_arg_t *arg = (tracker_arg_t *)ptr;
#ifdef HIGH_WATER_MARK
    int hwm = HIGH_WATER_MARK;
#endif
    if (!arg) {
        log_err("invalid argumuent");
        return NULL;
    }
    log_func("addr=%s", arg->addr);
    id = arg->id;
    context = zmq_ctx_new();
    if (MULTICAST_PUSH == arg->type) {
        socket = zmq_socket(context, ZMQ_PULL);
#ifdef HIGH_WATER_MARK
        zmq_setsockopt(socket, ZMQ_RCVHWM, &hwm, sizeof(hwm));
#endif
        ret = zmq_bind(socket, arg->addr);
    } else {
        socket = zmq_socket(context, ZMQ_SUB);
#ifdef HIGH_WATER_MARK
        zmq_setsockopt(socket, ZMQ_RCVHWM, &hwm, sizeof(hwm));
#endif
        ret = zmq_connect(socket, arg->addr);
        if (!ret) {
            if (zmq_setsockopt(socket, ZMQ_SUBSCRIBE, "", 0)) {
                log_err("failed to set socket");
                assert(0);
            }
        }
    }
    if (!ret) {
        while (true) {
            if (!msg)
                msg = zmsg_recv(socket);
            tracker_recv_lock(id);
            if (tracker_status.liveness[id] == ALIVE) {
                generator_handle(id, msg);
                tracker_recv_unlock(id);
                msg = NULL;
            } else {
                tracker_recv_unlock(id);
                ev_wait(&tracker_status.ev_live[id]);
            }
        }
    } else
        log_err("failed to connect (addr=%s)", arg->addr);
    zmq_close(socket);
    zmq_ctx_destroy(context);
    free(arg);
    return NULL;
}


void tracker_connect(int id)
{
    pthread_t thread;
    tracker_arg_t *arg;
    pthread_attr_t attr;

    arg = (tracker_arg_t *)calloc(1, sizeof(tracker_arg_t));
    if (!arg) {
        log_err("no memory");
        return;
    }
    arg->id = id;
    arg->type = MULTICAST;
    if (MULTICAST == MULTICAST_PUSH)
        tcpaddr(arg->addr, inet_ntoa(get_addr()), tracker_port + id);
    else if (MULTICAST == MULTICAST_PGM)
        pgmaddr(arg->addr, nodes[id], notifier_port);
    else if (MULTICAST == MULTICAST_EPGM)
        epgmaddr(arg->addr, nodes[id], notifier_port);
    else if (MULTICAST == MULTICAST_SUB)
        tcpaddr(arg->addr, nodes[id], notifier_port);
    else
        tcpaddr(arg->addr, nodes[id], tracker_port);
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM);
    pthread_create(&thread, &attr, tracker_do_connect, arg);
    pthread_attr_destroy(&attr);
}


void tracker_connect_peers()
{
    for (int i = 0; i < nr_nodes; i++)
        if (i != node_id)
            tracker_connect(i);
}


rep_t tracker_responder(req_t req)
{
    int i;
    char *paddr;
    rep_t rep = 0;
    struct in_addr addr;

    memcpy(&addr, &req, sizeof(struct in_addr));
    paddr = inet_ntoa(addr);
    log_func("addr=%s", paddr);
    for (i = 0; i < nr_nodes; i++) {
        if (!strcmp(paddr, nodes[i])) {
            tracker_connect(i);
            break;
        }
    }
    if (i == nr_nodes) {
        log_err("failed to connect");
        req = -1;
    }
    return rep;
}


int tracker_create_responder()
{
    pthread_t thread;
    pthread_attr_t attr;
    responder_arg_t *arg;

    arg = (responder_arg_t *)calloc(1, sizeof(responder_arg_t));
    if (!arg) {
        log_err("no memeory");
        return -ENOMEM;
    }
    arg->responder = tracker_responder;
    tcpaddr(arg->addr, inet_ntoa(get_addr()), tracker_port);
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM);
    pthread_create(&thread, &attr, responder_start, arg);
    pthread_attr_destroy(&attr);
    return 0;
}


void *tracker_handler(void *arg)
{
    while (true) {
        bool in = tracker_check_input();
        bool out = tracker_check_output();

        if (!in && !out)
            ev_wait(&tracker_status.ev_deliver);
    }
}


void tracker_create_handler()
{
    pthread_t thread;
    pthread_attr_t attr;

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM);
    pthread_create(&thread, &attr, tracker_handler, NULL);
}


void *tracker_monitor(void *arg)
{
    while (true) {
        usleep(TRACKER_CHECK_INTV);
        if (tracker_status.busy)
            tracker_status.busy = false;
        else {
            bool show = false;

            for (int id = 0; id < nr_nodes; id++) {
                bool deliver = false;
                struct list_head *pos;
                struct list_head *head = &tracker_status.checked[id];
                struct list_head *candidates = &tracker_status.candidates[id];

                tracker_mutex_lock();
                for (pos = candidates->next; pos != candidates; pos = pos->next) {
                    record_t *rec = list_entry(pos, record_t, cand[id]);

                    if (!is_delivered(rec)) {
                        if (!rec->prev[id] && !rec->count[id]) {
                            rec->count[id] = true;
                            rec->perceived++;
                            if (tracker_can_deliver(rec)) {
                                tracker_deliver(rec);
                                deliver = true;
                                break;
                            }
                        }
                        if (is_empty(&rec->checked[id]))
                            break;
                    }
                }
                if (!deliver) {
                    if (!list_empty(head)) {
                        record_t *rec = list_entry(head->prev, record_t, checked[id]);

                        pos = rec->cand[id].next;
                        head = head->prev;
                        show = true;
                    } else
                        pos = candidates->next;
                    tracker_check_queue(id, head, pos, false);
                }
                tracker_mutex_unlock();
            }
            if (show)
                show_status();
        }
    }
}


void tracker_create_queue_checker()
{
    pthread_t thread;
    pthread_attr_t attr;

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM);
    pthread_create(&thread, &attr, tracker_monitor, NULL);
}


int tracker_create()
{
    if (tracker_init()) {
        log_err("failed to initialize");
        return -EINVAL;
    }
#ifdef TRACKER_QUEUE_CHECKER
    tracker_create_queue_checker();
#endif
    tracker_create_handler();
    if ((MULTICAST == MULTICAST_SUB) || (MULTICAST == MULTICAST_PGM) || (MULTICAST == MULTICAST_EPGM))
        tracker_create_responder();
    else
        tracker_connect_peers();
#ifdef EVALUATE
    eval_create();
#endif
    return 0;
}


bool tracker_is_empty()
{
    for (int i = 0; i < nr_nodes; i++) {
        tracker_lock(i);
        bool empty = (queue_length(i) == 0);
        tracker_unlock(i);
        if (!empty)
            return false;
    }
    return true;
}


bool tracker_drain()
{
    if (tracker_is_empty() && batch_drain())
        return tracker_is_empty();
    else
        return false;
}
