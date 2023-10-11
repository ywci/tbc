#ifndef _EV_H
#define _EV_H

#include <tbc.h>

#define EV_SEC       1000000000 // nsec
#define EV_NOTIMEOUT -1

typedef struct {
    int sec;
    int nsec;
    bool wait;
    timeout_t timeout;
    pthread_cond_t cond;
    pthread_mutex_t mutex;
} ev_t;

void ev_set(ev_t *ev);
int ev_wait(ev_t *ev);
void ev_clear(ev_t *ev);
int ev_init(ev_t *ev, timeout_t timeout);

#endif
