#include "verify.h"

#define COUNTER_START 1

#define verify_tree_create rb_tree_new
#define verify_node_lookup rb_tree_find
#define verify_node_insert rb_tree_insert

typedef rbtree_t verify_tree_t;
typedef rbtree_node_t verify_node_t;

typedef struct {
    hdr_t hdr;
    verify_node_t node;
} verify_record_t;

struct {
    verify_tree_t input;
    verify_tree_t output;
    verify_tree_t timestamp;
} verify_status;

int verify_node_compare(const void *h1, const void *h2)
{
    hdr_t *hdr1 = (hdr_t *)h1;
    hdr_t *hdr2 = (hdr_t *)h2;

    assert(hdr1 && hdr2);
    return hdr1->hid == hdr2->hid ? 0 : (hdr1->hid > hdr2->hid ? 1 : -1);
}


inline verify_record_t *verify_lookup(verify_tree_t *tree, hdr_t *hdr)
{
    verify_node_t *node = NULL;

    if (!verify_node_lookup(tree, hdr, &node))
        return tree_entry(node, verify_record_t, node);
    else
        return NULL;
}


void verify_add_record(verify_tree_t *tree, hdr_t *hdr)
{
    verify_record_t *rec = calloc(1, sizeof(verify_record_t));

    assert(rec);
    rec->hdr = *hdr;
    rec->hdr.cnt++;
    if (verify_node_insert(tree, &rec->hdr, &rec->node)) {
        log_err("failed to verify");
        assert(0);
    }
}


bool verify(verify_tree_t *tree, hdr_t *hdr, int *cnt)
{
    verify_record_t *rec = verify_lookup(tree, hdr);

    if (!rec)
        verify_add_record(tree, hdr);
    else {
        if (hdr->cnt == rec->hdr.cnt)
            rec->hdr.cnt++;
        else {
#ifdef RESET_COUNTER
            if (COUNTER_START == hdr->cnt) {
                rec->hdr.cnt = hdr->cnt + 1;
            } else
#endif
            {
                *cnt = rec->hdr.cnt;
                return false;
            }
        }
    }
    return true;
}


void verify_input(zmsg_t *msg)
{
    int cnt = -1;
    hdr_t *hdr = get_hdr(msg);

    if (!verify(&verify_status.input, hdr, &cnt)) {
        if (!hdr->cnt)
            log_enable();
        else
            log_err(">> invalid input << expected=%d, current=%d, hid=%x", cnt, hdr->cnt, hdr->hid);
    }
}


void verify_output(hdr_t *hdr)
{
    int cnt = -1;

    if (!verify(&verify_status.output, hdr, &cnt)) {
        if (!hdr->cnt)
            log_enable();
        else
            log_err(">> invalid output << expected=%d, current=%d, hid=%x", cnt, hdr->cnt, hdr->hid);
    }
}


void verify_init()
{
    if (verify_tree_create(&verify_status.input, verify_node_compare))
        log_err("failed to create");

    if (verify_tree_create(&verify_status.output, verify_node_compare))
        log_err("failed to create");

    if (verify_tree_create(&verify_status.timestamp, verify_node_compare))
        log_err("failed to create");
}
