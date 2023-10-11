#include "ev.h"
#include "log.h"

int ev_init(ev_t *ev, timeout_t timeout)
{
    if (timeout > 0) {
        pthread_condattr_t attr;
        pthread_condattr_init(&attr);
#ifdef LINUX
        if (pthread_condattr_setclock(&attr, CLOCK_MONOTONIC)) {
            log_err("failed to initialize");
            return -1;
        }
#endif
        ev->timeout = timeout;
        ev->sec = timeout / EV_SEC;
        ev->nsec = timeout % EV_SEC;
        pthread_cond_init(&ev->cond, &attr);
    } else {
        ev->sec = 0;
        ev->nsec = 0;
        ev->timeout = 0;
        pthread_cond_init(&ev->cond, NULL);
    }
    pthread_mutex_init(&ev->mutex, NULL);
    ev->wait = false;
    return 0;
}


void ev_clear(ev_t *ev)
{
    pthread_mutex_lock(&ev->mutex);
    ev->wait = false;
    pthread_mutex_unlock(&ev->mutex);
}


void ev_set(ev_t *ev)
{
    pthread_mutex_lock(&ev->mutex);
    if (!ev->wait)
        ev->wait = true;
    else
        pthread_cond_broadcast(&ev->cond);
    pthread_mutex_unlock(&ev->mutex);
}


int ev_wait(ev_t *ev)
{
    int ret = 0;

    pthread_mutex_lock(&ev->mutex);
    if (!ev->wait) {
        ev->wait = true;
        if (ev->timeout) {
            unsigned long tmp;
            struct timespec timeout;

            clock_gettime(ev->timeout, &timeout);
            tmp = timeout.tv_nsec + ev->nsec;
            if (tmp >= EV_SEC) {
                timeout.tv_sec += 1 + ev->sec;
                timeout.tv_nsec = tmp - EV_SEC;
            } else {
                timeout.tv_sec += ev->sec;
                timeout.tv_nsec = tmp;
            }
            ret = pthread_cond_timedwait(&ev->cond, &ev->mutex, &timeout);
        } else
            pthread_cond_wait(&ev->cond, &ev->mutex);
        ev->wait = false;
    } else
        ev->wait = false;
    pthread_mutex_unlock(&ev->mutex);
    return ret;
}
