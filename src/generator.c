#include "batch.h"
#include "record.h"
#include "generator.h"
#include "responder.h"
#include "collector.h"
#include "heartbeat.h"
#include "publisher.h"
#include "subscriber.h"
#include "tracker.h"
#ifdef VERIFY
#include "verify.h"
#endif

#define GENERATOR_QUEUE_LEN 1000000

typedef struct {
    zmsg_t *msg;
    struct list_head list;
} generator_record_t;

struct {
    int count;
    bool active;
    bool filter;
    sender_desc_t desc;
    pthread_cond_t cond;
    pthread_mutex_t lock;
    host_time_t t_filter;
    pthread_mutex_t mutex;
    struct list_head queue;
} generator_status;

#define generator_need_save() (!generator_status.t_filter.sec || !generator_status.t_filter.usec)

void send_message(zmsg_t *msg)
{
    publish(&generator_status.desc, msg);
}


inline void generator_lock()
{
    pthread_mutex_lock(&generator_status.lock);
}


inline void generator_unlock()
{
    pthread_mutex_unlock(&generator_status.lock);
}


inline int generator_time_compare(host_time_t *t1, host_time_t *t2)
{
    if (t1->sec > t2->sec)
        return 1;
    else if (t1->sec == t2->sec) {
        if (t1->usec > t2->usec)
            return 1;
        else if (t1->usec == t2->usec)
            return 0;
        else
            return -1;
    } else
        return -1;
}


inline void generator_do_handle(zmsg_t *msg)
{
    zmsg_t *ret = batch(msg);
    
    if (ret)
        send_message(ret);
}


void generator_suspend()
{
    crash_details("start");
    generator_lock();
    generator_status.active = false;
    generator_unlock();
    crash_details("finished!");
}


void generator_do_suspend()
{
    pthread_cond_t *cond = &generator_status.cond;
    pthread_mutex_t *mutex = &generator_status.mutex;

    generator_unlock();
    pthread_mutex_lock(mutex);
    pthread_cond_wait(cond, mutex);
    pthread_mutex_unlock(mutex);
}


void generator_wakeup()
{
    pthread_cond_t *cond = &generator_status.cond;
    pthread_mutex_t *mutex = &generator_status.mutex;

    pthread_mutex_lock(mutex);
    pthread_cond_signal(cond);
    pthread_mutex_unlock(mutex);
}


void generator_resume()
{
    generator_record_t *rec;
    generator_record_t *next;

    crash_details("start");
    generator_lock();
    generator_status.active = true;
    memset(&generator_status.t_filter, 0, sizeof(host_time_t));
    list_for_each_entry_safe(rec, next, &generator_status.queue, list) {
        zmsg_t *msg = rec->msg;
        host_time_t *t = (host_time_t *)get_timestamp(msg);

        generator_do_handle(msg);
        list_del(&rec->list);
        free(rec);
    }
    generator_status.count = 0;
    generator_unlock();
    crash_details("finished!");
}


void generator_start_filter(host_time_t bound)
{
    log_func("bound=<sec: %d, usec: %d>, session=%d", bound.sec, bound.usec, get_session(node_id));
    generator_lock();
    generator_status.t_filter = bound;
    generator_status.filter = true;
    generator_unlock();
    generator_wakeup();
}


void generator_stop_filter()
{
    log_func("session=%d", get_session(node_id));
    generator_lock();
    generator_status.filter = false;
    generator_unlock();
    generator_wakeup();
}


void generator_drain()
{
    log_func("drain (session=%d)", get_session(node_id));
    while(!tracker_drain());
}


zmsg_t *generator_check_msg(zmsg_t *msg)
{
    bool active;
    host_time_t *t = (host_time_t *)get_timestamp(msg);
retry:
    generator_lock();
    active = generator_status.active;
    if (active || (generator_status.filter && (generator_time_compare(&generator_status.t_filter, t) >= 0)))
        generator_do_handle(msg);
    else {
        if (generator_need_save()) {
            generator_record_t *rec = malloc(sizeof(generator_record_t));

            assert(rec);
            rec->msg = msg;
            list_add_tail(&rec->list, &generator_status.queue);
            assert(generator_status.count < GENERATOR_QUEUE_LEN);
            generator_status.count++;
        } else if (!active) {
            log_func("suspend (session=%d)", get_session(node_id));
            debug_crash_before_suspend();
            generator_do_suspend();
            goto retry;
        }
    }
    generator_unlock();
    return NULL;
}


void generator_handle(int id, zmsg_t *msg)
{
    if (is_batched(msg))
        batch_update(id, msg);
    else
        generator_do_handle(msg);
}


zmsg_t *generator_set_msg(zmsg_t *msg)
{
#ifdef VERIFY
    verify_input(msg);
#endif
    return generator_check_msg(msg);
}


rep_t generator_client_responder(req_t req)
{
    sub_arg_t *arg;
    pthread_t thread;
    struct in_addr addr;
    pthread_attr_t attr;

    memcpy(&addr, &req, sizeof(struct in_addr));
    log_func("addr=%s", inet_ntoa(addr));
    arg = (sub_arg_t *)malloc(sizeof(sub_arg_t));
    if (!arg) {
        log_err("no memory");
        return -ENOMEM;
    }
    if (MULTICAST == MULTICAST_PGM)
        pgmaddr(arg->src, inet_ntoa(addr), client_port);
    else if (MULTICAST == MULTICAST_EPGM)
        epgmaddr(arg->src, inet_ntoa(addr), client_port);
    else
        tcpaddr(arg->src, inet_ntoa(addr), client_port);
    strcpy(arg->dest, GENERATOR_ADDR);
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM);
    pthread_create(&thread, &attr, subscriber_start, arg);
    pthread_attr_destroy(&attr);
    return 0;
}


int generator_create_responder()
{
    pthread_t thread;
    pthread_attr_t attr;
    responder_arg_t *arg;

    arg = (responder_arg_t *)calloc(1, sizeof(responder_arg_t));
    if (!arg) {
        log_err("no memeory");
        return -ENOMEM;
    }
    arg->responder = generator_client_responder;
    tcpaddr(arg->addr, inet_ntoa(get_addr()), generator_port);
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM);
    pthread_create(&thread, &attr, responder_start, arg);
    pthread_attr_destroy(&attr);
    return 0;
}


void generator_init()
{
    batch_init();
    generator_status.count = 0;
    generator_status.active = true;
    generator_status.filter = false;
    INIT_LIST_HEAD(&generator_status.queue);
    pthread_cond_init(&generator_status.cond, NULL);
    pthread_mutex_init(&generator_status.lock, NULL);
    pthread_mutex_init(&generator_status.mutex, NULL);
    memset(&generator_status.t_filter, 0, sizeof(host_time_t));
}


int generator_create()
{
    int i, j;
    pub_arg_t *arg;
    pthread_t thread;
    pthread_attr_t attr;

    generator_init();
    if (tracker_create()) {
        log_err("failed to create");
        return -EINVAL;
    }
    arg = (pub_arg_t *)calloc(1, sizeof(pub_arg_t));
    if (!arg) {
        log_err("no memeory");
        return -ENOMEM;
    }
    if (MULTICAST == MULTICAST_SUB) {
        strcpy(arg->src, GENERATOR_ADDR);
        tcpaddr(arg->addr, inet_ntoa(get_addr()), notifier_port);
    } else if (MULTICAST == MULTICAST_PGM) {
        strcpy(arg->src, GENERATOR_ADDR);
        pgmaddr(arg->addr, inet_ntoa(get_addr()), notifier_port);
    }  else if (MULTICAST == MULTICAST_EPGM) {
        strcpy(arg->src, GENERATOR_ADDR);
        epgmaddr(arg->addr, inet_ntoa(get_addr()), notifier_port);
    } else if (MULTICAST == MULTICAST_PUB) {
        tcpaddr(arg->src, inet_ntoa(get_addr()), generator_port);
        tcpaddr(arg->addr, inet_ntoa(get_addr()), tracker_port);
    } else if (MULTICAST == MULTICAST_PUSH)
        tcpaddr(arg->src, inet_ntoa(get_addr()), generator_port);
    for (i = 0, j = 0; i < nr_nodes; i++) {
        if (i != node_id) {
            if (MULTICAST == MULTICAST_PUSH) {
                tcpaddr(arg->dest[j], nodes[i], tracker_port + node_id);
                j++;
            } else if ((MULTICAST == MULTICAST_SUB) || (MULTICAST == MULTICAST_PGM) || (MULTICAST == MULTICAST_EPGM)) {
                tcpaddr(arg->dest[j], nodes[i], tracker_port);
                j++;
            }
        }
    }
    arg->total = j;
    arg->bypass = true;
    arg->callback = generator_set_msg;
    arg->desc = &generator_status.desc;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM);
    pthread_create(&thread, &attr, publisher_start, arg);
    pthread_attr_destroy(&attr);
    if ((MULTICAST ==  MULTICAST_SUB) || (MULTICAST == MULTICAST_PGM) || (MULTICAST == MULTICAST_EPGM))
        generator_create_responder();
    return 0;
}
