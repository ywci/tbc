#ifndef _GENERATOR_H
#define _GENERATOR_H

#include "util.h"

int generator_create();
void generator_drain();
void generator_resume();
void generator_suspend();
void generator_stop_filter();
void generator_handle(int id, zmsg_t *msg);
void generator_start_filter(host_time_t bound);

#endif
