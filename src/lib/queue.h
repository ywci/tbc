#ifndef _QUEUE_H
#define _QUEUE_H

#include "list.h"
#include "rbtree.h"
#include "record.h"

#define QUEUE_ENTRY_MAX 32
#define QUEUE_BLOCK_MAX 32
#define QUEUE_CHUNK_MAX 4096
#define QUEUE_LENGTH    10000000

typedef struct queue_item {
    seq_t seq;
    int count;
    int length;
    rbtree_t root;
    record_t *rec;
    rbtree_node_t node;
    struct list_head list;
    struct list_head head;
    struct queue_item *parent;
} queue_item_t;

#define queue_remove_list(l) do { \
    assert(is_valid(l)); \
    list_del(l); \
    set_empty(l); \
} while (0)

#define queue_rbtree_insert(p_root, p_key, p_node) do { \
    assert(p_key); \
    if (rb_tree_insert(p_root, p_key, p_node)) { \
        show_timestamp("Error:", -1, p_key); \
        log_err("failed to insert"); \
    } \
    assert((p_root)->root); \
} while (0)

#define queue_rbtree_rightmost(p_root, p_node) do { \
    assert((p_root)->root); \
    if (rb_tree_get_rightmost(p_root, p_node)) \
        log_err("failed to get rightmost"); \
} while (0)

#define queue_rbtree_remove(p_root, p_node) do { \
    assert((p_root)->root); \
    rb_tree_remove(p_root, p_node); \
} while (0)

#define queue_item_counter_dec(item) do { \
    assert((item)->count > 0); \
    (item)->count--; \
    if ((item)->count > 0) \
        assert((item)->root.root); \
} while (0)

void queue_init();
int queue_length(int id);
struct list_head *queue_head(int id);
void queue_pop(int id, record_t *record);
void queue_push(int id, record_t *record, bool *earlist);
record_t *queue_update_prev(int id, record_t *prev, record_t *record);

#endif
