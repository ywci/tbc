#ifndef _UTIL_H
#define _UTIL_H

#include <zmq.h>
#include <czmq.h>
#include <errno.h>
#include <assert.h>
#include <net/if.h>
#include <stdlib.h>
#include <tbc.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "timestamp.h"
#include "queue.h"
#include "list.h"
#include "log.h"

#define get_time(t) gettimeofday(&(t), NULL)
#define addr2hid(addr) ((hid_t)(addr).s_addr)
#define get_timestamp(msg) ((timestamp_t *)zframe_data(zmsg_first(msg)))

#define pgmaddr(addr, orig, port) addr_convert("pgm", addr, orig, port)
#define epgmaddr(addr, orig, port) addr_convert("epgm", addr, orig, port)
#define tcpaddr(addr, orig, port) sprintf(addr, "tcp://%s:%d", orig, port)

#define is_delivered(rec) ((rec)->deliver)
#define is_empty(list) ((list)->next == NULL)
#define is_valid(list) ((list)->next != NULL)
#define set_empty(list) do { (list)->next = NULL; } while (0)

typedef void (*sender_t)(zmsg_t *);
typedef zmsg_t *(*callback_t)(zmsg_t *);

typedef struct sender_desc {
    int total;
    sender_t sender;
    void *desc[NODE_MAX];
} sender_desc_t;

typedef struct {
    timestamp_sec_t sec;
    timestamp_usec_t usec;
} host_time_t;

#define sndmsg(msg, socket) zmsg_send(msg, socket)

#define assert_list_add_tail(ent, head) do { \
    struct list_head *_ent = ent; \
    struct list_head *_head = head; \
    assert(_ent); \
    assert(_head); \
    assert(_head->next && _head->prev); \
    list_add_tail(_ent, _head); \
} while (0)

#define assert_list_add(ent, head) do { \
    struct list_head *_ent = ent; \
    struct list_head *_head = head; \
    assert(_ent); \
    assert(_head); \
    assert(_head->next && _head->prev); \
    list_add(_ent, _head); \
} while (0)

hid_t get_hid();
void check_settings();
void init_func_timer();
struct in_addr get_addr();
int get_bits(uint64_t val);
void publish(sender_desc_t *sender, zmsg_t *msg);
uint64_t time_diff(timeval_t *start, timeval_t *end);
void addr_convert(const char *protocol, char *dest, char *src, int port);
void forward(void *frontend, void *backend, callback_t callback, sender_desc_t *sender);

#ifdef LOG_AFTER_CRASH
#define debug_log_after_crash log_enable
#else
#define debug_log_after_crash(...) do {} while (0)
#endif

#ifdef QUIET_AFTER_RECYCLE
#define debug_quiet_after_recycle log_disable
#else
#define debug_quiet_after_recycle(...) do {} while (0)
#endif

#ifdef QUIET_AFTER_RESUME
#define debug_quiet_after_resume log_disable
#else
#define debug_quiet_after_resume(...) do {} while (0)
#endif

#ifdef CRASH_AFTER_RESUME
#define debug_crash_after_resume() exit(-1)
#else
#define debug_crash_after_resume() do {} while (0)
#endif

#ifdef CRASH_BEFORE_SUSPEND
#define debug_crash_before_suspend() exit(-1)
#else
#define debug_crash_before_suspend() do {} while (0)
#endif

#ifdef SIMU_CRASH
#define debug_crash_simu() { \
    if (node_id < CRASH_NODES) { \
        static int cnt = 0; \
        cnt++; \
        if (cnt == CRASH_AFTER_N_REQ) \
            exit(-1); \
    } \
}
#else
#define debug_crash_simu() do {} while (0)
#endif

#ifdef SLOW_DOWN_AFTER_CRASH
#define debug_slow_down_after_crash() do { \
    if (log_is_valid()) { \
        log_func("slowing down ..."); \
        sleep(30); \
    } \
} while (0)
#else
#define debug_slow_down_after_crash(...) do {} while (0)
#endif

void func_timer_start(const char *func_name);
void func_timer_stop(const char *func_name);

#ifdef FUNC_TIMER
#define track_enter_call(func) do { \
    func_timer_start(__func__); \
    func(); \
} while (0)
#define track_enter() func_timer_start(__func__)
#define track_exit_call(func) do { \
    func_timer_stop(__func__); \
    func(); \
} while (0)
#define track_exit(func) func_timer_stop(__func__)
#else
#define track_enter_call(func) func()
#define track_exit_call(func) func()
#define track_enter() do {} while (0)
#define track_exit() do {} while (0)
#endif

#endif
