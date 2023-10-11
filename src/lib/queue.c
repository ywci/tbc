#include "queue.h"
#include "util.h"
#include "timestamp.h"

#define QUEUE_ASSERT

struct {
    queue_item_t queues[NODE_MAX];
} queue_status;

static inline int queue_item_compare(const void *t1, const void *t2)
{
    return -timestamp_compare(t1, t2);
}


void queue_init()
{
    for (int i = 0; i < NODE_MAX; i++) {
        queue_item_t *queue = &queue_status.queues[i];

        memset(queue, 0, sizeof(queue_item_t));
        INIT_LIST_HEAD(&queue->head);
        INIT_LIST_HEAD(&queue->list);
        rb_tree_new(&queue->root, queue_item_compare);
    }
}


static inline queue_item_t *queue_new_item(int id, queue_item_t *parent, record_t *record, const int size)
{
    if (parent->count < size) {
        queue_item_t *item = calloc(1, sizeof(queue_item_t));

        item->rec = record;
        INIT_LIST_HEAD(&item->head);
        list_add_tail(&item->list, &parent->head);
        rb_tree_new(&item->root, queue_item_compare);
        queue_rbtree_insert(&parent->root, record->timestamp, &item->node);
        item->parent = parent;
        parent->count++;
        return item;
    } else {
        log_err("too many items");
        return NULL;
    }
}


static inline queue_item_t *queue_new_chunk(int id, queue_item_t *queue, record_t *record)
{
    queue_item_t *chunk = queue_new_item(id, queue, record, QUEUE_CHUNK_MAX);

    if (chunk && (queue->count != 1))
        if (timestamp_compare(queue->rec->timestamp, record->timestamp) > 0)
            queue->rec = record;

    show_queue(id, record, "queue_count=%d", queue->count);
    return chunk;
}


static inline queue_item_t *queue_new_block(int id, queue_item_t *chunk, record_t *record)
{
    queue_item_t *block = queue_new_item(id, chunk, record, QUEUE_BLOCK_MAX);

    if (block && (chunk->count != 1)) {
        timestamp_t *timestamp = record->timestamp;

        if (timestamp_compare(chunk->rec->timestamp, timestamp) > 0) {
            queue_item_t *queue = chunk->parent;

            queue_rbtree_remove(&queue->root, &chunk->node);
            queue_rbtree_insert(&queue->root, timestamp, &chunk->node);
            if (timestamp_compare(queue->rec->timestamp, timestamp) > 0)
                queue->rec = record;
            chunk->rec = record;
        }
    }
    show_queue(id, record, "chunk_count=%d", chunk->count);
    return block;
}


static inline void queue_add_item(int id, queue_item_t *block, record_t *record)
{
    timestamp_t *timestamp = record->timestamp;

    if (block->rec != record) {
        assert(block->rec);
        if (timestamp_compare(block->rec->timestamp, timestamp) > 0) {
            queue_item_t *chunk = block->parent;

            assert(block->rec);
            if ((chunk->rec == block->rec) || (timestamp_compare(chunk->rec->timestamp, timestamp) > 0)) {
                queue_item_t *queue = chunk->parent;

                assert(queue->rec);
                if ((queue->rec == chunk->rec) || (timestamp_compare(queue->rec->timestamp, timestamp) > 0))
                    queue->rec = record;

                queue_rbtree_remove(&queue->root, &chunk->node);
                queue_rbtree_insert(&queue->root, timestamp, &chunk->node);
                chunk->rec = record;
            }
            queue_rbtree_remove(&chunk->root, &block->node);
            queue_rbtree_insert(&chunk->root, timestamp, &block->node);
            block->rec = record;
        }
    }
    block->count++;
    record->block[id] = block;
    list_add_tail(&record->item_list[id], &block->head);
    queue_rbtree_insert(&block->root, timestamp, &record->item_node[id]);
    show_queue(id, record, "block_count=%d (block=0x%llx)", block->count, (unsigned long long)block);
}


static inline void queue_add(int id, queue_item_t *queue, record_t *record)
{
    queue_item_t *chunk;
    queue_item_t *block;
    struct list_head *head = &queue->head;

    if (!list_empty(head)) {
        chunk = list_entry(head->prev, queue_item_t, list);
        assert(!list_empty(&chunk->head));
        block = list_entry(chunk->head.prev, queue_item_t, list);
        if (block->count == QUEUE_ENTRY_MAX) {
            if (chunk->count == QUEUE_BLOCK_MAX) {
                chunk = queue_new_chunk(id, queue, record);
                block = queue_new_block(id, chunk, record);
            } else
                block = queue_new_block(id, chunk, record);
        }
    } else {
        chunk = queue_new_chunk(id, queue, record);
        block = queue_new_block(id, chunk, record);
        queue->rec = record;
    }
    queue_add_item(id, block, record);
}


record_t *queue_update_prev(int id, record_t *prev, record_t *curr)
{
    struct list_head *i;
    struct list_head *j;
    struct list_head *k;
    queue_item_t *block = prev->block[id];
    queue_item_t *chunk = block->parent;
    queue_item_t *queue = chunk->parent;
    timestamp_t *timestamp = curr->timestamp;

    for (i = prev->item_list[id].prev; i != &block->head; i = i->prev) {
        record_t *rec = list_entry(i, record_t, item_list[id]);

        if (timestamp_compare(rec->timestamp, timestamp) < 0) {
            show_prev_str(id, curr, rec, "find prev item in the same block");
            return rec;
        }
    }
    for (i = block->list.prev; i != &chunk->head; i = i->prev) {
        block = list_entry(i, queue_item_t, list);
        if (timestamp_compare(block->rec->timestamp, timestamp) < 0) {
            for (j = block->head.prev; j != &block->head; j = j->prev) {
                record_t *rec = list_entry(j, record_t, item_list[id]);

                if (timestamp_compare(rec->timestamp, timestamp) < 0) {
                    show_prev_str(id, curr, rec, "find prev item in the same chunk");
                    return rec;
                }
            }
            log_err("failed to find prev item in the same chunk");
        }
    }
    for (i = chunk->list.prev; i != &queue->head; i = i->prev) {
        chunk = list_entry(i, queue_item_t, list);
        if (timestamp_compare(chunk->rec->timestamp, timestamp) < 0) {
            for (j = chunk->head.prev; j != &chunk->head; j = j->prev) {
                block = list_entry(j, queue_item_t, list);
                if (timestamp_compare(block->rec->timestamp, timestamp) < 0) {
                    for (k = block->head.prev; k != &block->head; k = k->prev) {
                        record_t *rec = list_entry(k, record_t, item_list[id]);

                        if (timestamp_compare(rec->timestamp, timestamp) < 0) {
                            show_prev_str(id, curr, rec, "find prev item");
                            return rec;
                        }
                    }
                    log_err("failed to find prev item");
                }
            }
            log_err("failed to find prev item");
        }
    }
    return NULL;
}


static inline void queue_push_item(int id, queue_item_t *queue, record_t *record, bool earliest)
{
    struct list_head *head = &queue->head;

    if (!list_empty(head) && !earliest) {
        struct list_head *i;
        timestamp_t *timestamp = record->timestamp;

        for (i = head->prev; i != head; i = i->prev) {
            queue_item_t *chunk = list_entry(i, queue_item_t, list);

            if (timestamp_compare(chunk->rec->timestamp, timestamp) < 0) {
                struct list_head *j;

                for (j = chunk->head.prev; j != &chunk->head; j = j->prev) {
                    queue_item_t *block = list_entry(j, queue_item_t, list);

                    if (timestamp_compare(block->rec->timestamp, timestamp) < 0) {
                        struct list_head *k;

                        for (k = block->head.prev; k != &block->head; k = k->prev) {
                            record_t *rec = list_entry(k, record_t, item_list[id]);

                            if (timestamp_compare(rec->timestamp, timestamp) < 0) {
                                if (is_empty(&rec->next[id]))
                                    INIT_LIST_HEAD(&rec->next[id]);
                                list_add_tail(&record->link[id], &rec->next[id]);
                                record->prev[id] = rec;
                                show_prev(id, record, rec, NULL);
                                break;
                            }
                        }
                        assert(k != &block->head);
                        break;
                    }
                }
                assert(j != &chunk->head);
                break;
            }
        }
        assert(i != head);
    }
    queue->seq++;
    queue->length++;
    queue_add(id, queue, record);
}


void queue_push(int id, record_t *record, bool *earliest)
{
    queue_item_t *queue = &queue_status.queues[id];

    assert(record->timestamp);
    if (queue->length < QUEUE_LENGTH) {
        record_t *rec = queue->rec;

        if (!rec || (timestamp_compare(record->timestamp, rec->timestamp) < 0)) {
            queue_push_item(id, queue, record, true);
            *earliest = true;
        } else
            queue_push_item(id, queue, record, false);
        show_queue(id, record, "len=%d seq=%d (id=%d)", queue->length, queue->seq, id);
    } else
        log_err("no space, len=%d, seq=%d (id=%d)", queue->length, queue->seq, id);
}



static inline void queue_del_item(queue_item_t *item)
{
    queue_item_t *parent = item->parent;
    struct list_head *list = &item->list;

    assert(!item->count);
    queue_remove_list(list);
    queue_item_counter_dec(parent);
    rb_tree_destroy(&item->root);
    free(item);
}


static inline void queue_update_chunk(int id, queue_item_t *chunk, record_t *record)
{
    rbtree_node_t *node = NULL;
    queue_item_t *block = NULL;
    queue_item_t *queue = chunk->parent;

    assert(chunk->count > 0);
    queue_rbtree_rightmost(&chunk->root, &node);
    block = list_entry(node, queue_item_t, node);
    assert(block->count > 0);
    assert(chunk->rec != block->rec);
    chunk->rec = block->rec;
    queue_rbtree_remove(&queue->root, &chunk->node);
    queue_rbtree_insert(&queue->root, block->rec->timestamp, &chunk->node);
    assert(queue->rec);
    if (queue->rec == record) {
        queue_rbtree_rightmost(&queue->root, &node);
        chunk = list_entry(node, queue_item_t, node);
        assert(queue->rec != chunk->rec);
        queue->rec = chunk->rec;
        show_queue(id, chunk->rec, "update chunk_rec and queue_rec");
    } else
        show_queue(id, chunk->rec, "update chunk_rec only");
}


static inline void queue_remove_block_item(int id, queue_item_t *block, record_t *record)
{
    record_t *rec = NULL;
    queue_item_t *chunk = block->parent;

    queue_rbtree_remove(&block->root, &record->item_node[id]);
    queue_remove_list(&record->item_list[id]);
    if (block->count > 1) {
        rbtree_node_t *node;

        queue_rbtree_rightmost(&block->root, &node);
        rec = list_entry(node, record_t, item_node[id]);
#ifdef QUEUE_ASSERT
        if (timestamp_compare(rec->timestamp, record->timestamp) <= 0) {
            show_prev(id, rec, record, NULL);
            log_err("failed to remove record");
        }
#endif
    }
    queue_item_counter_dec(block);
    block->rec = rec;
    queue_rbtree_remove(&chunk->root, &block->node);
    if (rec) {
        queue_rbtree_insert(&chunk->root, rec->timestamp, &block->node);
        if (chunk->rec == record)
            queue_update_chunk(id, chunk, record);
    } else if (chunk->count > 1) {
        assert(!block->count);
        if (chunk->rec == record)
            queue_update_chunk(id, chunk, record);
        queue_del_item(block);
    } else {
        queue_item_t *queue = chunk->parent;

        assert(!block->count);
        assert(queue->count > 0);
        assert(1 == chunk->count);
        assert(chunk->rec == record);

        queue_rbtree_remove(&queue->root, &chunk->node);
        if (queue->rec == chunk->rec) {
            if (queue->count > 1) {
                queue_item_t *nu;
                rbtree_node_t *node;

                queue_rbtree_rightmost(&queue->root, &node);
                nu = list_entry(node, queue_item_t, node);
                assert(nu->rec != queue->rec);
                queue->rec = nu->rec;
                show_queue(id, queue->rec, "queue_rec is updated, queue_count=%d, chunk_count=%d, block_count=%d", queue->count, chunk->count, block->count);
            } else {
                queue->rec = NULL;
                show_queue(id, record, "queue_rec is empty, queue_count=%d, chunk_count=%d, block_count=%d", queue->count, chunk->count, block->count);
            }
        }
        queue_del_item(block);
        queue_del_item(chunk);
    }
}


static inline void queue_remove_item(int id, queue_item_t *block, record_t *record)
{
    show_queue(id, record, "block_count=%d (block=0x%llx)", block->count, (unsigned long long)block);
    assert(block->count > 1);
    queue_rbtree_remove(&block->root, &record->item_node[id]);
    queue_remove_list(&record->item_list[id]);
    queue_item_counter_dec(block);
}


static inline void queue_remove(int id, record_t *record)
{
    queue_item_t *block = record->block[id];

    if (block->rec == record)
        queue_remove_block_item(id, block, record);
    else
        queue_remove_item(id, block, record);
    record->block[id] = NULL;
}


void queue_pop(int id, record_t *record)
{
    queue_item_t *queue = &queue_status.queues[id];

    if (queue->length > 0) {
        queue_remove(id, record);
        queue->length--;
        show_queue(id, record, "len=%d seq=%d (id=%d)", queue->length, queue->seq, id);
    } else
        log_err("no item, seq=%d (id=%d)", queue->seq, id);
}


struct list_head *queue_head(int id)
{
    return &queue_status.queues[id].list;
}


int queue_length(int id)
{
    return queue_status.queues[id].length;
}
