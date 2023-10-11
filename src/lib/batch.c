#include "ev.h"
#include "batch.h"
#include "verify.h"
#include "tracker.h"

#define BATCH_DEP_MTX
// #define BATCH_FAST_UPDATE

#define BATCH_MAX           100           // msg
#define BATCH_SEND_INTV     10000         // nsec
#define BATCH_CHECK_INTV    1000          // nsec
#define BATCH_RECYCLE_INTV  1000          // nsec
#define BATCH_FORWARD_INTV  10000000      // usec
#define BATCH_CLEAN_INTV    EV_NOTIMEOUT
#define BATCH_NR_TIMESTAMPS (BATCH_MAX + 10000)

#define batch_list_del list_del
#define batch_list_add assert_list_add_tail

#define batch_tree_create rb_tree_new
#define batch_node_lookup rb_tree_find
#define batch_node_remove rb_tree_remove
#define batch_node_insert rb_tree_insert

#define batch_prev batch_status.prev
#define batch_tail batch_status.tail
#define batch_total batch_status.total
#define batch_bufsz batch_status.bufsz
#define batch_ts batch_timestamps.buffer
#define batch_matrix batch_status.matrix
#define batch_ev_send batch_status.ev_send
#define batch_progress batch_status.progress
#define batch_sessions batch_status.sessions
#define batch_pkt_header batch_status.pkt_header
#define batch_ev_recycle batch_status.ev_recycle
#define batch_ev_cleaner batch_status.ev_cleaner
#define batch_ev_checker batch_status.ev_checker
#define batch_dep batch_timestamps.pkt_header.dep
#define batch_count batch_timestamps.pkt_header.count
#define batch_session batch_timestamps.pkt_header.session
#define batch_recycle_counter batch_status.recycle_counter
#define batch_record_has_receiver(rec, id) ((rec)->receivers & node_mask[id])
#define batch_record_set_receiver(rec, id) ((rec)->receivers |= node_mask[id])
#define batch_clean_complete(rec) (((rec)->clean & available_nodes) == available_nodes)
#define batch_receive_complete(rec) (((rec)->receivers & available_nodes) == available_nodes)
#ifdef BATCH_DEP_MTX
#define batch_dep_matrix batch_status.dep_matrix
#endif

typedef rbtree_t batch_tree_t;
typedef uint32_t batch_version_t;
typedef rbtree_node_t batch_node_t;
typedef struct list_head batch_list_t;

typedef enum {
    BATCH_TIMEOUT_INIT = 0,
    BATCH_TIMEOUT_SET,
    BATCH_TIMEOUT_CLEAR,
} batch_timeout_t;

typedef struct {
    seq_t dep[NODE_MAX * NODE_MAX];
    session_t session;
    uint32_t count;
} batch_pkt_header_t;

typedef struct batch_record {
    zmsg_t *msg;
    bitmap_t clean;
    timestamp_t ts;
    batch_node_t node;
    bitmap_t receivers;
    seq_t seq[NODE_MAX];
    batch_list_t recycle;
    bool visible[NODE_MAX];
    timestamp_t *timestamp;
    batch_list_t list[NODE_MAX];
#ifdef FORWARD
    batch_timeout_t timeout;
#endif
} batch_record_t;

struct {
    int bufsz;
    int total;
    ev_t ev_send;
    timeval_t time;
    ev_t ev_recycle;
    ev_t ev_cleaner;
    ev_t ev_checker;
    seq_t *progress;
    char *pkt_header;
    batch_tree_t tree;
    int recycle_counter;
    batch_list_t recycle;
    pthread_rwlock_t lock;
    seq_t *matrix[NODE_MAX];
    batch_list_t head[NODE_MAX];
    batch_list_t *tail[NODE_MAX];
    batch_list_t *prev[NODE_MAX];
    session_t sessions[NODE_MAX];
    pthread_mutex_t recycle_lock;
    pthread_rwlock_t list_locks[NODE_MAX];
#ifdef BATCH_DEP_MTX
    seq_t *dep_matrix[NODE_MAX][NODE_MAX];
    seq_t dep[NODE_MAX][NODE_MAX * NODE_MAX];
#else
    seq_t dep[NODE_MAX * NODE_MAX];
#endif
} batch_status;

struct {
    batch_pkt_header_t pkt_header;
    timestamp_t buffer[BATCH_NR_TIMESTAMPS];
} batch_timestamps;

int batch_row_size = -1;
int batch_dep_size = -1;
int batch_pkt_header_off = -1;
int batch_pkt_header_size = -1;

inline void batch_wrlock();
inline void batch_rdlock();
inline void batch_unlock();
static inline void batch_release(batch_record_t *rec);
static inline void batch_add(int id, timestamp_t *timestamp, zmsg_t *msg);

session_t get_session(int id)
{
    session_t ret;

    batch_rdlock();
    if (id != node_id)
        ret = batch_sessions[id];
    else
        ret = batch_session;
    batch_unlock();
    return ret;
}


void set_session(int id, session_t session)
{
    batch_wrlock();
    if (id != node_id)
        batch_sessions[id] = session;
    else
        batch_session = session;
    batch_unlock();
    log_func("session=%d (id=%d)", session, id);
}


bool get_seq_end(int id, seq_t *seq)
{
    bool match = false;
    batch_list_t *pos;
    batch_list_t *head = &batch_status.head[id];

    batch_rdlock();
    for (pos = head->prev; pos != head; pos = pos->prev) {
        batch_record_t *rec = list_entry(pos, batch_record_t, list[id]);

        if (rec->visible[id] && is_empty(&rec->recycle)) {
            *seq = rec->seq[id];
            match = true;
            break;
        }
    }
    batch_unlock();
    return match;
}


bool get_seq_start(int id, seq_t *seq)
{
    bool match = false;
    batch_list_t *pos;
    batch_list_t *head = &batch_status.head[id];

    batch_rdlock();
    for (pos = head->next; pos != head; pos = pos->next) {
        batch_record_t *rec = list_entry(pos, batch_record_t, list[id]);

        if (rec->visible[id] && is_empty(&rec->recycle)) {
            *seq = rec->seq[id];
            match = true;
            break;
        }
    }
    batch_unlock();
    return match;
}


void get_timestamps(int id, timestamp_t *timestamps, seq_t start, seq_t end)
{
    seq_t seq = 0;
    batch_list_t *pos;
    batch_record_t *rec = NULL;
    timestamp_t *p = timestamps;
    batch_list_t *head = &batch_status.head[id];

    log_info("get timestamps ... (id=%d)", id);
    batch_rdlock();
    for (pos = head->next; pos != head; pos = pos->next) {
        rec = list_entry(pos, batch_record_t, list[id]);
        if (rec->visible[id] && is_empty(&rec->recycle)) {
            if (!seq) {
                seq = rec->seq[id];
                assert(seq == start);
            } else {
                assert(seq + 1 == rec->seq[id]);
                seq = rec->seq[id];
            }
            memcpy(p, rec->timestamp, sizeof(timestamp_t));
            p++;
        } else if (seq) {
            assert(is_empty(&rec->recycle));
            break;
        }
    }
    batch_unlock();
    log_info("finished getting timestamps, start=%d, end=%d (id=%d)", start, end, id);
    assert(rec && rec->seq[id] == end);
}


void add_timestamps(int id, timestamp_t *timestamps, int count, zmsg_t *msg)
{
    for (int i = 0; i < count; i++)
        batch_add(id, &timestamps[i], msg);
    ev_set(&batch_ev_checker);
    ev_set(&batch_ev_cleaner);
    show_header(id, &batch_timestamps.pkt_header);
    debug_slow_down_after_crash();
    zmsg_destroy(&msg);
}


inline void batch_wrlock()
{
    pthread_rwlock_wrlock(&batch_status.lock);
}


inline void batch_rdlock()
{
    pthread_rwlock_rdlock(&batch_status.lock);
}


inline void batch_unlock()
{
    pthread_rwlock_unlock(&batch_status.lock);
}


inline void batch_recycle_lock()
{
    pthread_mutex_lock(&batch_status.recycle_lock);
}


inline void batch_recycle_unlock()
{
    pthread_mutex_unlock(&batch_status.recycle_lock);
}


inline void batch_list_wrlock(int id)
{
    pthread_rwlock_wrlock(&batch_status.list_locks[id]);
}


inline void batch_list_rdlock(int id)
{
    pthread_rwlock_rdlock(&batch_status.list_locks[id]);
}


inline void batch_list_unlock(int id)
{
    pthread_rwlock_unlock(&batch_status.list_locks[id]);
}


static inline void batch_push(int id, batch_record_t *rec)
{
    batch_list_t *entry = &rec->list[id];
    batch_list_t *head = &batch_status.head[id];

    track_enter();
    batch_list_wrlock(id);
    batch_progress[id]++;
    rec->seq[id] = batch_progress[id];
    batch_list_add(entry, head);
    batch_record_set_receiver(rec, id);
    batch_list_unlock(id);
    track_exit();
}


inline batch_record_t *batch_lookup(batch_tree_t *tree, timestamp_t *timestamp)
{
    batch_node_t *node = NULL;

    if (!batch_node_lookup(tree, timestamp, &node))
        return tree_entry(node, batch_record_t, node);
    else
        return NULL;
}


bool batch_is_visible(int id, batch_record_t *record)
{
    int cnt = 0;
    seq_t seq = record->seq[id];

    if ((seq > 0) && batch_record_has_receiver(record, node_id)) {
        for (int i = 0; i < nr_nodes; i++) {
            if (seq <= batch_matrix[i][id]) {
                cnt++;
                if (cnt >= majority) {
                    show_visible(id, record);
                    return true;
                }
            }
        }
    }
    return false;
}


int batch_node_compare(const void *t1, const void *t2)
{
    assert(t1 && t2);
    return timestamp_compare(t1, t2);
}


zmsg_t *batch_pack()
{
    zmsg_t *msg = NULL;
    static seq_t progress[NODE_MAX * NODE_MAX] = {0};

    batch_wrlock();
    assert(batch_count <= BATCH_NR_TIMESTAMPS);
    if (batch_count) {
        size_t size = batch_pkt_header_size + batch_count * sizeof(timestamp_t);
        zframe_t *frame = zframe_new(batch_pkt_header, size);

        batch_count = 0;
        batch_unlock();
        msg = zmsg_new();
        zmsg_prepend(msg, &frame);
        show_header(node_id, &batch_timestamps.pkt_header);
#ifdef BATCH_DEP_MTX
        show_pack(batch_dep_matrix, true);
#endif
    } else if (memcmp(progress, batch_pkt_header, batch_dep_size)) {
        zframe_t *frame = zframe_new(batch_pkt_header, batch_pkt_header_size);

        memcpy(progress, batch_pkt_header, batch_dep_size);
        batch_unlock();
        msg = zmsg_new();
        zmsg_prepend(msg, &frame);
        show_header(node_id, &batch_timestamps.pkt_header);
#ifdef BATCH_DEP_MTX
        show_pack(batch_dep_matrix, false);
#endif
    } else
        batch_unlock();
    return msg;
}


void *batch_sender(void *arg)
{
    while (true) {
        zmsg_t *msg = batch_pack();

        if (msg)
            send_message(msg);
        else
            ev_wait(&batch_ev_send);
    }
    return NULL;
}


#ifdef FORWARD
void *batch_forwarder(void *arg)
{
    batch_list_t *head = &batch_status.head[node_id];

    while (true) {
        batch_list_t *pos;

        usleep(BATCH_FORWARD_INTV);
        track_enter();
        batch_list_rdlock(node_id);
        list_for_each(pos, head) {
            batch_record_t *rec = list_entry(pos, batch_record_t, list[node_id]);

            switch (rec->timeout) {
            case BATCH_TIMEOUT_INIT:
                rec->timeout = BATCH_TIMEOUT_SET;
                break;
            case BATCH_TIMEOUT_SET:
                if (!batch_receive_complete(rec)) {
                    if (rec->msg) {
                        show_timestamp(">> forward <<", -1, rec->timestamp);
                        send_message(zmsg_dup(rec->msg));
                    }
                } else
                    rec->timeout = BATCH_TIMEOUT_CLEAR;
                break;
            default:
                break;
            }
        }
        batch_list_unlock(node_id);
        track_exit();
    }
}
#endif


void batch_recycle(batch_record_t *rec)
{
    show_timestamp(">> recycle <<", -1, rec->timestamp);
    for (int i = 0; i < nr_nodes; i++) {
        batch_list_t *list = &rec->list[i];

        if (is_valid(list)) {
            batch_list_wrlock(i);
            if (is_valid(list)) {
                if (batch_prev[i] == list)
                    batch_prev[i] = list->prev;
                if (batch_tail[i] == list)
                    batch_tail[i] = list->prev;
                batch_list_del(list);
            }
            batch_list_unlock(i);
        }
    }
    batch_wrlock();
    batch_node_remove(&batch_status.tree, &rec->node);
    batch_bufsz--;
    batch_unlock();
    zmsg_destroy(&rec->msg);
    free(rec);
    debug_quiet_after_recycle();
    debug_crash_simu();
}


void batch_do_release(batch_record_t *rec)
{
    batch_list_t *list = &rec->recycle;

    if (is_empty(list)) {
        batch_recycle_lock();
        if (is_empty(list)) {
            show_timestamp(">> release <<", -1, rec->timestamp);
            batch_list_add(list, &batch_status.recycle);
            batch_recycle_counter++;
            ev_set(&batch_ev_recycle);
            ev_set(&batch_ev_cleaner);
        }
        batch_recycle_unlock();
    }
}


static inline void batch_release(batch_record_t *rec)
{
    track_enter();
    batch_do_release(rec);
    track_exit();
}


void batch_remove(timestamp_t *timestamp)
{
    batch_node_t *node = NULL;

    if (!batch_node_lookup(&batch_status.tree, timestamp, &node)) {
        batch_record_t *rec = tree_entry(node, batch_record_t, node);

        batch_release(rec);
    }
}


static inline void batch_do_handle(zmsg_t *msg, timestamp_t *timestamp, batch_record_t *rec)
{
    bool valid = timestamp_check(timestamp);
    
    track_enter();
    if (!rec) {
        if (valid) {
            rec = calloc(1, sizeof(batch_record_t));
            rec->msg = msg;
            rec->timestamp = timestamp;
            if (batch_node_insert(&batch_status.tree, rec->timestamp, &rec->node)) {
                log_err("failed to handle");
                assert(0);
            }
            batch_push(node_id, rec);
        }
    } else {
        if (valid) {
            if (!batch_record_has_receiver(rec, node_id)) {
                rec->msg = msg;
                batch_push(node_id, rec);
            } else {
                log_func("find a duplicated message");
                show_timestamp("|--> >>duplicated<<", -1, timestamp);
                valid = false;;
            }
        } else {
            log_func("find an invalid message");
            show_timestamp("|--> >>invalid<<", -1, timestamp);
        }
    }
    if (valid) {
        batch_ts[batch_count] = *timestamp;
        batch_total++;
        batch_count++;
        batch_bufsz++;
        if (batch_count >= BATCH_MAX)
            ev_set(&batch_ev_send);
        show_batch(-1, rec);
    } else {
        log_func("find an expired message");
        show_timestamp("|--> >>expired<<", -1, timestamp);
        zmsg_destroy(&msg);
    }
    track_exit();
}


static inline void batch_handle(zmsg_t *msg)
{
    timestamp_t *timestamp = get_timestamp(msg);
    track_enter_call(batch_wrlock);
    batch_record_t *rec = batch_lookup(&batch_status.tree, timestamp);
    batch_do_handle(msg, timestamp, rec);
    track_exit_call(batch_unlock);
}


zmsg_t *batch(zmsg_t *msg)
{
    batch_handle(msg);
    debug_slow_down_after_crash();
    return NULL;
}


inline void batch_put(int id, batch_record_t *rec)
{
    track_enter();
    tracker_update(id, rec->timestamp, rec->msg);
    rec->visible[id] = true;
    track_exit();
}


void batch_check(int id)
{
    batch_list_t *pos;
    batch_record_t *rec;
    batch_list_t *head = &batch_status.head[id];

    track_enter();
    batch_list_rdlock(id);
    for (pos = batch_prev[id]->next; pos != head; pos = pos->next) {
        rec = list_entry(pos, batch_record_t, list[id]);
        assert(!rec->visible[id]);
        if (batch_is_visible(id, rec))
            batch_put(id, rec);
        else {
            batch_prev[id] = pos->prev;
            break;
        }
    }
    if (pos == head)
        batch_prev[id] = head->prev;
    batch_list_unlock(id);
    track_exit();
}


void *batch_checker(void *arg)
{
    long id = (long)arg;

    while (true) {
        ev_wait(&batch_ev_checker);
        batch_check(id);
        debug_slow_down_after_crash();
    }
    return NULL;
}


bool batch_can_clean(int id, batch_record_t *rec)
{
    seq_t seq = rec->seq[id];

    assert(batch_progress[id] >= seq);
#ifdef BATCH_DEP_MTX
    for (int i = 0; i < nr_nodes; i++) {
        if (alive_node[i]) {
            for (int j = 0; j < nr_nodes; j++) {
                if (alive_node[j] && (batch_dep_matrix[i][j][id] < seq)) {
                    log_func("cannot clean, dep[%d][%d][%d]=%d, seq=%d (id=%d)", i, j, id, batch_dep_matrix[i][j][id], seq, id);
                    return false;
                }
            }
        }
    }
#else
    for (int i = 0; i < nr_nodes; i++) {
        if (alive_node[i] && (batch_matrix[i][id] < seq)) {
            log_func("cannot clean, dep[%d][%d]=%d, seq=%d", i, id, batch_matrix[i][id], seq);
            return false;
        }
    }
#endif
    return true;
}


void batch_clean(int id)
{
    batch_list_t *pos;
    batch_list_t *tail;
    bool clean = false;
    batch_record_t *rec;
    batch_list_t *head = &batch_status.head[id];

    batch_list_rdlock(id);
    tail = batch_tail[id];
    if (!list_empty(tail)) {
        for (pos = tail->next; pos != head; pos = pos->next) {
            bitmap_t mask = node_mask[id];

            rec = list_entry(pos, batch_record_t, list[id]);
            assert(!(rec->clean & mask));
            if (batch_can_clean(id, rec)) {
                clean = true;
                rec->clean |= mask;
                show_cleaner(">> clean <<", id, rec);
            } else
                break;
        }
        batch_tail[id] = pos->prev;
    }
    batch_list_unlock(id);
    if (clean)
        ev_set(&batch_ev_recycle);
}


void *batch_cleaner(void *arg)
{
    long id = (long)arg;

    while (true) {
        ev_wait(&batch_ev_cleaner);
        batch_clean(id);
    }
    return NULL;
}


void *batch_recycler(void *arg)
{
    while (true) {
        int cnt = 0;
        batch_list_t *pos;
        batch_list_t *tail;
        batch_list_t *head = &batch_status.recycle;

        ev_wait(&batch_ev_recycle);
        batch_recycle_lock();
        pos = head->next;
        tail = head->prev;
        batch_recycle_unlock();
        if ((tail != head) && (pos != tail)) {
            batch_list_t *i;

            for (i = pos->next; i != tail; pos = i, i = i->next) {
                batch_record_t *rec = list_entry(pos, batch_record_t, recycle);

                if (batch_receive_complete(rec) && batch_clean_complete(rec)) {
                    batch_list_del(pos);
                    batch_recycle(rec);
                    cnt++;
                }
            }
        }
        if (cnt) {
            batch_recycle_lock();
            assert(batch_recycle_counter >= cnt);
            batch_recycle_counter -= cnt;
            batch_recycle_unlock();
        }
    }
}


static inline void batch_insert_timestamp(int id, timestamp_t *timestamp, zmsg_t *msg, batch_record_t *rec, bool valid)
{
    track_enter();
    if (valid) {
        rec = calloc(1, sizeof(batch_record_t));
        rec->timestamp = &rec->ts,
        rec->ts = *timestamp;
        if (batch_node_insert(&batch_status.tree, rec->timestamp, &rec->node)) {
            log_err("failed to insert");
            assert(0);
        }
        batch_push(id, rec);
        show_batch(id, rec);
    } else {
        log_func("find an expired message (released), id=%d", id);
        show_timestamp("|--> >>expired<<", -1, timestamp);
    }
    track_exit();
}


static inline void batch_do_update_timestamp(int id, timestamp_t *timestamp, zmsg_t *msg, batch_record_t *rec)
{
    track_enter();
    if (!batch_record_has_receiver(rec, id)) {
        batch_push(id, rec);
        show_batch(id, rec);
    } else {
        log_func("find a duplicated message, id=%d", id);
        show_timestamp("|--> >>duplicated<<", -1, timestamp);
    }
    track_exit();
}


static inline void batch_update_timestamp(int id, timestamp_t *timestamp, zmsg_t *msg, batch_record_t *rec, bool valid)
{
    track_enter();
    if (valid)
        batch_do_update_timestamp(id, timestamp, msg, rec);
    else {
        log_func("find an expired message, id=%d", id);
        show_timestamp("|--> >>expired<<", -1, timestamp);
        batch_push(id, rec);
        batch_do_release(rec);
    }
    track_exit();
}


static inline void batch_add(int id, timestamp_t *timestamp, zmsg_t *msg)
{
    assert((id >= 0) && (id < nr_nodes) && (id != node_id) && timestamp);
    track_enter_call(batch_wrlock);
    bool valid = timestamp_check(timestamp);
    batch_record_t *rec = batch_lookup(&batch_status.tree, timestamp);

    if (!rec)
        batch_insert_timestamp(id, timestamp, msg, rec, valid);
    else
        batch_update_timestamp(id, timestamp, msg, rec, valid);
    track_exit_call(batch_unlock);
}


inline bool batch_unpack(int id, void *buf, timestamp_t **first, int *count, seq_t **dep)
{
    batch_pkt_header_t *head = (batch_pkt_header_t *)((char *)buf - batch_pkt_header_off);
    session_t session = *(&head->session);

    if (session == batch_sessions[id]) {
        *dep = (seq_t *)buf;
        *count = *(&head->count);
        *first = (timestamp_t *)&head[1];
        return true;
    } else {
        log_debug("dropped, session=%d (current=%d)", session, batch_sessions[id]);
        return false;
    }
}


void batch_update_dep(int id, seq_t *dep)
{
#ifdef BATCH_DEP_MTX
    seq_t *ptr = dep;

    for (int i = 0; i < nr_nodes; i++) {
#ifdef BATCH_FAST_UPDATE
        memcpy(batch_dep_matrix[id][i], ptr, batch_row_size);
#else
        for (int j = 0; j < nr_nodes; j++)
            if (batch_dep_matrix[id][i][j] < ptr[j])
                batch_dep_matrix[id][i][j] = ptr[j];
#endif
        ptr += nr_nodes;
    }
    show_dep(id, batch_dep_matrix, NULL);
#else
#ifdef BATCH_FAST_UPDATE
    memcpy(batch_matrix[id], dep, batch_row_size);
#else
    for (int i = 0; i < nr_nodes; i++)
        if (batch_matrix[id][i] < dep[i])
            batch_matrix[id][i] = dep[i];
#endif
#endif
}


void batch_update(int id, zmsg_t *msg)
{
    int count = 0;
    seq_t *dep = NULL;
    timestamp_t *timestamps = NULL;
    zframe_t *frame = zmsg_first(msg);
    char *buf = (char *)zframe_data(frame);

    if (batch_unpack(id, buf, &timestamps, &count, &dep)) {
        batch_update_dep(id, dep);
        add_timestamps(id, timestamps, count, msg);
    }
}


void batch_create_sender()
{
    pthread_t thread;
    pthread_attr_t attr;

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM);
    pthread_create(&thread, &attr, batch_sender, NULL);
}


void batch_create_recycler()
{
    pthread_t thread;
    pthread_attr_t attr;

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM);
    pthread_create(&thread, &attr, batch_recycler, NULL);
}


void batch_create_cleaners()
{
    for (long i = 0; i < nr_nodes; i++) {
        pthread_t thread;
        pthread_attr_t attr;

        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM);
        pthread_create(&thread, &attr, batch_cleaner, (void *)i);
    }
}


#ifdef FORWARD
void batch_create_forwarder()
{
    pthread_t thread;
    pthread_attr_t attr;

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM);
    pthread_create(&thread, &attr, batch_forwarder, NULL);
}
#endif


void batch_create_checkers()
{
    for (long i = 0; i < nr_nodes; i++) {
        pthread_t thread;
        pthread_attr_t attr;

        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM);
        pthread_create(&thread, &attr, batch_checker, (void *)i);
    }
}


void batch_init()
{
    int off;
    const size_t sz = NODE_MAX * NODE_MAX * sizeof(seq_t);

    batch_row_size = nr_nodes * sizeof(seq_t);
#ifdef BATCH_DEP_MTX
    batch_dep_size = nr_nodes * batch_row_size;
    off = NODE_MAX * NODE_MAX - nr_nodes * nr_nodes;
#else
    batch_dep_size = batch_row_size;
    off += NODE_MAX * NODE_MAX - nr_nodes;
#endif
    for (int i = 0; i < nr_nodes; i++) {
        INIT_LIST_HEAD(&batch_status.head[i]);
        batch_prev[i] = &batch_status.head[i];
        batch_tail[i] = &batch_status.head[i];
        pthread_rwlock_init(&batch_status.list_locks[i], NULL);
#ifdef BATCH_DEP_MTX
        for (int j = 0; j < nr_nodes; j++) {
            if ((j != i) && (i != node_id))
                batch_dep_matrix[i][j] = &batch_status.dep[i][j * nr_nodes];
            else
                batch_dep_matrix[i][j] = &batch_dep[off + j * nr_nodes];
        }
        batch_matrix[i] = &batch_dep[off + i * nr_nodes];
        memset(batch_status.dep[i], 0, sz);
#else
        if (i != node_id)
            batch_matrix[i] = &batch_status.dep[i * nr_nodes];
        else
            batch_matrix[i] = &batch_dep[off];
#endif
    }
    if (batch_tree_create(&batch_status.tree, batch_node_compare))
        log_err("failed to create");
    batch_total = 0;
    batch_bufsz = 0;
    batch_count = 0;
    batch_session = 0;
    batch_recycle_counter = 0;
    batch_pkt_header_off = off * sizeof(seq_t);
    batch_pkt_header = (char *)&batch_timestamps + batch_pkt_header_off;
    batch_pkt_header_size = sizeof(batch_pkt_header_t) - batch_pkt_header_off;
    assert(batch_pkt_header == (char *)&batch_dep[off]);
#ifdef BATCH_DEP_MTX
    batch_progress = batch_dep_matrix[node_id][node_id];
#else
    batch_progress = batch_matrix[node_id];
    memset(batch_status.dep, 0, sz);
#endif
    ev_init(&batch_ev_send, BATCH_SEND_INTV);
    ev_init(&batch_ev_cleaner, BATCH_CLEAN_INTV);
    ev_init(&batch_ev_checker, BATCH_CHECK_INTV);
    ev_init(&batch_ev_recycle, BATCH_RECYCLE_INTV);
    memset(batch_dep, 0, sz);
    get_time(batch_status.time);
    INIT_LIST_HEAD(&batch_status.recycle);
    pthread_rwlock_init(&batch_status.lock, NULL);
    pthread_mutex_init(&batch_status.recycle_lock, NULL);
    batch_create_recycler();
    batch_create_checkers();
    batch_create_cleaners();
    batch_create_sender();
#ifdef FORWARD
    batch_create_forwarder();
#endif
}


bool batch_drain()
{
    for (int i = 0; i < nr_nodes; i++) {
        if (available_nodes & node_mask[i]) {
            batch_list_rdlock(i);
            bool empty = list_empty(&batch_status.head[i]);
            batch_list_unlock(i);
            if (!empty)
                return false;
        }
    }
    return true;
}
