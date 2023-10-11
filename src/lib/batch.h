#ifndef _BATCH_H
#define _BATCH_H

#include "util.h"

#define is_batched(msg) (1 == zmsg_size(msg))

void batch_init();
bool batch_drain();
void batch_wrlock();
void batch_unlock();
zmsg_t *batch(zmsg_t *msg);
void batch_update(int id, zmsg_t *msg);
void batch_remove(timestamp_t *timestamp);

#endif
