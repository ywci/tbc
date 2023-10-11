#include "timestamp.h"
#include "util.h"

#define NR_TS_GROUPS 1024

#define ts_tree_create rb_tree_new
#define ts_node_lookup rb_tree_find
#define ts_node_insert rb_tree_insert
#define ts_lock_init pthread_rwlock_init
#define ts_hash(timestamp) (timestamp->hid % NR_TS_GROUPS)
#define ts_group_wrlock(grp) pthread_rwlock_wrlock(&(grp)->lock)
#define ts_group_rdlock(grp) pthread_rwlock_rdlock(&(grp)->lock)
#define ts_group_unlock(grp) pthread_rwlock_unlock(&(grp)->lock)
#define ts_group(timestamp) (&timestamp_status.ts_groups[ts_hash(timestamp)])

typedef rbtree_t ts_tree_t;
typedef rbtree_node_t ts_node_t;
typedef pthread_rwlock_t ts_lock_t;

typedef struct {
    ts_tree_t tree;
    ts_lock_t lock;
} ts_group_t;

typedef struct {
    ts_node_t node;
    timestamp_t timestamp;
} ts_t;

struct {
    ts_group_t ts_groups[NR_TS_GROUPS];
} timestamp_status;

int timestamp_compare(const void *t1, const void *t2)
{
    timestamp_t *ts1 = (timestamp_t *)(t1);
    timestamp_t *ts2 = (timestamp_t *)(t2);
    uint32_t usec1 = ts1->usec;
    uint32_t usec2 = ts2->usec;
    uint32_t sec1 = ts1->sec;
    uint32_t sec2 = ts2->sec;
    hid_t hid1 = ts1->hid;
    hid_t hid2 = ts2->hid;

    if (sec1 > sec2)
        return 1;
    else if (sec1 == sec2) {
        if (usec1 > usec2)
            return 1;
        else if (usec1 == usec2) {
            if (hid1 > hid2)
                return 1;
            else if (hid1 == hid2)
                return 0;
            else
                return -1;
        } else
            return -1;
    } else
        return -1;
}


int timestamp_node_compare(const void *t1, const void *t2)
{
    timestamp_t *ts1 = (timestamp_t *)(t1);
    timestamp_t *ts2 = (timestamp_t *)(t2);
    hid_t hid1 = ts1->hid;
    hid_t hid2 = ts2->hid;

    return hid1 == hid2 ? 0 : (hid1 > hid2 ? 1 : -1);
}


inline void timestamp_add(ts_group_t *grp, timestamp_t *timestamp)
{
    ts_t *ts = calloc(1, sizeof(ts_t));

    ts->timestamp = *timestamp;
    if (ts_node_insert(&grp->tree, &ts->timestamp, &ts->node)) {
        log_err("failed to add");
        assert(0);
    }
}


inline ts_t *timestamp_lookup(ts_group_t *grp, timestamp_t *timestamp)
{
    ts_node_t *node = NULL;

    if (!ts_node_lookup(&grp->tree, timestamp, &node))
        return tree_entry(node, ts_t, node);
    else
        return NULL;
}


bool timestamp_update(timestamp_t *timestamp)
{
    ts_t *ts;
    bool ret = true;
    ts_group_t *grp = ts_group(timestamp);

    ts_group_wrlock(grp);
    ts = timestamp_lookup(grp, timestamp);
    if (!ts)
        timestamp_add(grp, timestamp);
    else {
        if (timestamp_compare(&ts->timestamp, timestamp) < 0)
            ts->timestamp = *timestamp;
        else {
            show_timestamp("***  expired  ***", -1, timestamp);
            ret = false;
        }
    }
out:
    ts_group_unlock(grp);
    return ret;
}


bool timestamp_check(timestamp_t *timestamp)
{
    bool ret;
    ts_t *ts;
    ts_group_t *grp = ts_group(timestamp);

    ts_group_rdlock(grp);
    ts = timestamp_lookup(grp, timestamp);
    if (!ts)
        ret = true;
    else {
        if (timestamp_compare(&ts->timestamp, timestamp) < 0)
            ret = true;
        else
            ret = false;
    }
    ts_group_unlock(grp);
    return ret;
}


void timestamp_init()
{
    for (int i = 0; i < NR_TS_GROUPS; i++) {
        if (ts_tree_create(&timestamp_status.ts_groups[i].tree, timestamp_node_compare))
            log_err("failed to create");
        ts_lock_init(&timestamp_status.ts_groups[i].lock, NULL);
    }
}
