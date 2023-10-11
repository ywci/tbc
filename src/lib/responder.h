#ifndef _RESPONDER_H
#define _RESPONDER_H

#include "util.h"

typedef rep_t (*responder_t)(req_t);

typedef struct {
    char addr[ADDR_SIZE];
    responder_t responder;
} responder_arg_t;

void *responder_start(void *ptr);

#endif
