#ifndef _TRACKER_H
#define _TRACKER_H

#include "util.h"

int tracker_create();
bool tracker_drain();
void tracker_wakeup();
void tracker_recover(int id);
void tracker_suspect(int id);
liveness_t tracker_get_liveness(int id);
void tracker_set_liveness(int id, liveness_t liveness);
void tracker_update(int id, timestamp_t *timestamp, zmsg_t *msg);

#endif
