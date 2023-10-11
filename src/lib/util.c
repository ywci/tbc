#include "util.h"
#include "ev.h"

#define FUNC_TIMER_MAX 32
const timeout_t FUNC_TIMEOUT_ERROR   = 10 * (timeout_t)EV_SEC;
const timeout_t FUNC_TIMEOUT_WARNING = 5000000;      // usec

typedef struct {
    bool enter;
    timeval_t t;
    ev_t ev_wakeup;
    ev_t ev_timeout;
    char *func_name;
    struct list_head list;
} func_timer_t;

struct {
    int total;
    struct list_head head;
    pthread_mutex_t mutex;
} func_timer_status;

void addr_convert(const char *protocol, char *dest, char *src, int port)
{
    int i;
    char buf[sizeof(struct in_addr)];

    if (!inet_aton(src, (struct in_addr *)buf)) {
        log_err("failed to convert address %s", src);
        return;
    }

    i = buf[3];
    if ((i == 0) || (i > MULTICAST_MAX)) {
        log_err("failed to convert address %s", src);
        return;
    }

    if (!strcmp(protocol, "pgm"))
        sprintf(dest, "pgm://%s;%s%d:%d", inet_ntoa(get_addr()), MULTICAST_NET, i, port);
    else if (!strcmp(protocol, "epgm"))
        sprintf(dest, "epgm://%s;%s%d:%d", iface, MULTICAST_NET, i, port);
    else
        log_err("failed to convert address, invalid protocol %s, addr=%s", protocol, src);
}


void publish(sender_desc_t *sender, zmsg_t *msg)
{
    int i;

    for (i = 0; i < sender->total - 1; i++) {
        zmsg_t *dup = zmsg_dup(msg);

        sndmsg(&dup, sender->desc[i]);
    }

    sndmsg(&msg, sender->desc[i]);
}


void forward(void *src, void *dest, callback_t callback, sender_desc_t *sender)
{
    while (true) {
        zmsg_t *msg = zmsg_recv(src);

        if (callback)
            msg = callback(msg);

        if (msg) {
            if (sender) {
                if (sender->sender)
                    sender->sender(msg);
                else
                    publish(sender, msg);
            } else
                sndmsg(&msg, dest);
        }
    }
}


struct in_addr get_addr()
{
    int fd;
    struct ifreq ifr;

    fd = socket(AF_INET, SOCK_DGRAM, 0);
    ifr.ifr_addr.sa_family = AF_INET;
    strncpy(ifr.ifr_name, iface, IFNAMSIZ - 1);
    ioctl(fd, SIOCGIFADDR, &ifr);
    close(fd);
    return ((struct sockaddr_in *)&ifr.ifr_addr)->sin_addr;
}


uint64_t time_diff(timeval_t *start, timeval_t *end)
{
    return (end->tv_sec - start->tv_sec) * 1000000 + end->tv_usec - start->tv_usec;
}


hid_t get_hid()
{
    struct in_addr addr = get_addr();

    return addr2hid(addr);
}


void init_func_timer()
{
    func_timer_status.total = 0;
    INIT_LIST_HEAD(&func_timer_status.head);
    pthread_mutex_init(&func_timer_status.mutex, NULL);
}


void func_timer_lock()
{
    pthread_mutex_lock(&func_timer_status.mutex);
}


void func_timer_unlock()
{
    pthread_mutex_unlock(&func_timer_status.mutex);
}


void *func_timer(void *arg)
{
    func_timer_t *timer = (func_timer_t *)arg;

    while (true) {
        int ret;

        ev_wait(&timer->ev_wakeup);
        ret = ev_wait(&timer->ev_timeout);
        if (ETIMEDOUT == ret)
            log_info("[%s] timeout", timer->func_name);
    }

    return NULL;
}


func_timer_t *func_timer_create(const char *func_name)
{
    pthread_t thread;
    pthread_attr_t attr;
    func_timer_t *timer = NULL;

    assert(func_timer_status.total < FUNC_TIMER_MAX);
    func_timer_status.total++;

    timer = malloc(sizeof(func_timer_t));
    timer->enter = false;
    timer->func_name = (char *)func_name;
    ev_init(&timer->ev_wakeup, EV_NOTIMEOUT);
    ev_init(&timer->ev_timeout, FUNC_TIMEOUT_ERROR);
    list_add_tail(&timer->list, &func_timer_status.head);

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM);
    pthread_create(&thread, &attr, func_timer, timer);
    pthread_attr_destroy(&attr);

    return timer;
}


void func_timer_start(const char *func_name)
{
    func_timer_t *ent;
    bool match = false;
    struct list_head *head = &func_timer_status.head;

    func_timer_lock();
    list_for_each_entry(ent, head, list) {
        if (ent->func_name == func_name) {
            match = true;
            break;
        }
    }

    if (!match)
        ent = func_timer_create(func_name);


    if (!ent->enter) {
        ent->enter = true;
        get_time(ent->t);
        ev_set(&ent->ev_wakeup);
    }
    func_timer_unlock();
}


void func_timer_stop(const char *func_name)
{
    timeout_t t;
    timeval_t current;
    func_timer_t *ent;
    bool match = false;
    struct list_head *head = &func_timer_status.head;

    func_timer_lock();
    list_for_each_entry(ent, head, list) {
        if (ent->func_name == func_name) {
            match = true;
            break;
        }
    }
    assert(match);
    if (ent->enter) {
        ev_set(&ent->ev_timeout);
        ent->enter = false;
        get_time(current);
        t = time_diff(&ent->t, &current);
        if (t >= FUNC_TIMEOUT_WARNING)
            log_info("[%s] timeout", ent->func_name);
    }
    func_timer_unlock();
}


int get_bits(uint64_t val)
{
    int cnt = 0;

    while (val != 0) {
        val >>= 1;
        cnt++;
    }
    return cnt;
}


void check_settings()
{
    if (EVAL_SMPL && (get_bits(EVAL_SMPL) + 1 != get_bits(EVAL_SMPL + 1)))
        log_err("invalid setting of EVAL_SMPL, EVAL_SMPL=%d", EVAL_SMPL);

    if (EVAL_SMPL && (EVAL_INTV <= EVAL_SMPL))
        log_err("invalid setting of EVAL_INTV, EVAL_INTV=%d", EVAL_INTV);
}
