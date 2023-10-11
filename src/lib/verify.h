#ifndef VERIFY_H
#define VERIFY_H

#include "util.h"

typedef struct {
    uint64_t hid;
    int cnt;
    struct timeval t;
} hdr_t;

#define get_hdr(msg) ((hdr_t *)zframe_data(zmsg_last(msg)))
#define get_ts(msg) ((timestamp_t *)zframe_data(zmsg_last(msg)))

#ifdef COUNT
#define ts2hdr(ts, hdr) do {          \
    memset(hdr, 0, sizeof(hdr_t));    \
    hdr->hid = ts->hid;               \
    hdr->cnt = timestamp_counter(ts); \
} while (0)
#else
#define ts2hdr(ts, hdr) do {          \
    memset(hdr, 0, sizeof(hdr_t));    \
    hdr->hid = ts->hid;               \
    hdr->t.tv_sec = ts->sec;          \
    hdr->t.tv_usec = ts->usec;        \
} while (0)
#endif

void verify_init();
void verify_input(zmsg_t *msg);
void verify_output(hdr_t *hdr);

#endif
