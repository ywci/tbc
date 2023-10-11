#include "batch.h"
#include "record.h"
#include "timestamp.h"

#define NR_RECORD_GROUPS 1024

#define record_tree_create rb_tree_new
#define record_node_lookup rb_tree_find
#define record_node_insert rb_tree_insert
#define record_node_remove rb_tree_remove
#define record_lock_init pthread_mutex_init
#define record_group_lock(grp) pthread_mutex_lock(&(grp)->lock)
#define record_group_unlock(grp) pthread_mutex_unlock(&(grp)->lock)
#define record_hash(timestamp) ((timestamp)->usec % NR_RECORD_GROUPS)

struct {
    pthread_rwlock_t rwlock;
    pthread_rwlock_t deliver_lock;
    record_group_t record_groups[NR_RECORD_GROUPS];
} record_status;

int record_node_compare(const void *t1, const void *t2)
{
    assert(t1 && t2);
    return timestamp_compare(t1, t2);
}


inline void record_rdlock()
{
    pthread_rwlock_rdlock(&record_status.rwlock);
}


inline void record_wrlock()
{
    pthread_rwlock_wrlock(&record_status.rwlock);
}


inline void record_unlock()
{
    pthread_rwlock_unlock(&record_status.rwlock);
}


inline record_group_t *record_get_group(timestamp_t *timestamp)
{
    unsigned int n = record_hash(timestamp);

    assert(n < NR_RECORD_GROUPS);
    return &record_status.record_groups[n];
}


inline record_t *record_lookup(record_group_t *group, timestamp_t *timestamp)
{
    record_node_t *node = NULL;

    if (!record_node_lookup(&group->tree, timestamp, &node))
        return tree_entry(node, record_t, node);
    else
        return NULL;
}


void record_deliver(record_t *rec)
{
    record_group_t *group = record_get_group(rec->timestamp);

    timestamp_update(rec->timestamp);
    record_group_lock(group);
    rec->deliver = true;
    record_group_unlock(group);
}


record_t *record_add(record_group_t *group, zmsg_t *msg)
{
    zframe_t *frame;
    record_t *rec = (record_t *)calloc(1, sizeof(record_t));

    frame = zmsg_first(msg);
    rec->msg = msg;
    rec->group = group;
    rec->timestamp = (timestamp_t *)zframe_data(frame);
    if (record_node_insert(&group->tree, rec->timestamp, &rec->node)) {
        log_err("failed to insert");
        assert(0);
    }
    return rec;
}


void record_group_remove(record_group_t *group, record_t *rec)
{
    assert(rec);
    show_record(-1, rec);
    if (record_node_remove(&group->tree, &rec->node)) {
        log_err("failed to remove");
        assert(0);
    }
    free(rec);
}


record_t *record_find(int id, timestamp_t *timestamp, zmsg_t *msg)
{
    record_t *rec = NULL;
    record_group_t *group;
    bool available = node_mask[id] & available_nodes;

    record_rdlock();
    group = record_get_group(timestamp);
    record_group_lock(group);
    rec = record_lookup(group, timestamp);
    if (!rec) {
        if (!available || !msg)
            goto out;
        rec = record_add(group, msg);
        if (!rec) {
            log_err("failed to add record");
            goto out;
        }
    } else {
        if (is_delivered(rec)) {
            rec = NULL;
            goto out;
        }
    }
    show_record(id, rec);
out:
    record_group_unlock(group);
    if (!rec)
        record_unlock();
    return rec;
}


record_t *record_get(int id, zmsg_t *msg)
{
    zframe_t *frame;
    timestamp_t *timestamp;

    frame = zmsg_first(msg);
    assert(zframe_size(frame) == sizeof(timestamp_t));
    timestamp = (timestamp_t *)zframe_data(frame);
    return record_find(id, timestamp, msg);
}


void record_put(int id, record_t *record)
{
    record_unlock();
}


void record_release(record_t *record)
{
    record_group_t *group = record->group;
    timestamp_t *timestamp = record->timestamp;

    batch_wrlock();
    record_wrlock();
    record_group_lock(group);
    record_group_remove(group, record);
    record_group_unlock(group);
    record_unlock();
    batch_remove(timestamp);
    batch_unlock();
}


void record_init()
{
    for (int i = 0; i < NR_RECORD_GROUPS; i++) {
        if (record_tree_create(&record_status.record_groups[i].tree, record_node_compare))
            log_err("failed to create");
        record_lock_init(&record_status.record_groups[i].lock, NULL);
    }
    pthread_rwlock_init(&record_status.deliver_lock, NULL);
    pthread_rwlock_init(&record_status.rwlock, NULL);
}
