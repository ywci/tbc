#ifndef _TIMESTAMP_H
#define _TIMESTAMP_H

#include <tbc.h>

#ifdef COUNT
#define timestamp_set(timestamp, second, cnt) do { \
    timestamp->sec = second; \
    timestamp->usec = cnt; \
} while (0)
#else
#define timestamp_set(timestamp, time_val) do { \
    timestamp->sec = time_val.tv_sec; \
    timestamp->usec = time_val.tv_usec; \
} while (0)
#endif

#define timestamp_counter(timestamp) ((timestamp)->usec)

void timestamp_init();
bool timestamp_check(timestamp_t *timestamp);
bool timestamp_update(timestamp_t *timestamp);
int timestamp_compare(const void *t1, const void *t2);

#endif
