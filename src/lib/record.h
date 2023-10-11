#ifndef _RECORD_H
#define _RECORD_H

#include "util.h"

typedef rbtree_t record_tree_t;
typedef rbtree_node_t record_node_t;
typedef pthread_mutex_t record_lock_t;

typedef struct record_group {
    record_tree_t tree;
    record_lock_t lock;
} record_group_t;

struct queue_item;

typedef struct record {
    zmsg_t *msg;
    bool ignore;
    bool deliver;
    int perceived;
    bitmap_t receivers;
    record_node_t node;
    bool count[NODE_MAX];
    record_group_t *group;
    timestamp_t *timestamp;
    struct list_head output;
    struct record *prev[NODE_MAX];
    struct list_head req[NODE_MAX];
    struct list_head link[NODE_MAX];
    struct list_head next[NODE_MAX];
    struct list_head cand[NODE_MAX];
    struct list_head input[NODE_MAX];
    rbtree_node_t item_node[NODE_MAX];
    struct list_head checked[NODE_MAX];
    struct queue_item *block[NODE_MAX];
    struct list_head item_list[NODE_MAX];
} record_t;

void record_init();
void record_deliver(record_t *record);
void record_release(record_t *record);
record_t *record_get(int id, zmsg_t *msg);
void record_put(int id, record_t *record);
record_t *record_find(int id, timestamp_t *timestamp, zmsg_t *msg);

#endif
