#include "heartbeat.h"
#include "collector.h"
#include "requester.h"
#include "responder.h"

#define HEARTBEAT_COMMAND      1
#define HEARTBEAT_WAITTIME     2    // sec
#define HEARTBEAT_INTERVAL     1000 // msec
#define HEARTBEAT_RETRY_MAX    1

typedef struct heartbeat_arg {
    int id;
    char addr[ADDR_SIZE];
} heartbeat_arg_t;

void *heartbeat_handle(void *ptr)
{
    int cnt;
    int ret;
    rep_t rep;
    void *socket;
    void *context;
    req_t req = HEARTBEAT_COMMAND;
    long intv = HEARTBEAT_INTERVAL;
    long timeout = HEARTBEAT_INTERVAL / 1000;
    heartbeat_arg_t *arg = (heartbeat_arg_t *)ptr;

    assert((intv > 0) && (timeout > 0));
    ret = request(arg->addr, &req, &rep);
    if (ret || rep) {
        log_err("failed to start, addr=%s", arg->addr);
        return NULL;
    }
    while (true) {
        context = zmq_ctx_new();
        socket = zmq_socket(context, ZMQ_REQ);
        ret = zmq_connect(socket, arg->addr);
        if (ret) {
            log_err("failed to connect, addr=%s", arg->addr);
            sleep(HEARTBEAT_WAITTIME);
            continue;
        }
        cnt = 0;
        while (true) {
            zmq_pollitem_t item = {socket, 0, ZMQ_POLLIN, 0};

            ret = zmq_send(socket, &req, sizeof(req_t), 0);
            if (ret == sizeof(req_t)) {
                zmq_poll(&item, 1, intv);
                if (item.revents & ZMQ_POLLIN) {
                    ret = zmq_recv(socket, &rep, sizeof(rep_t), 0);
                    if ((ret == sizeof(rep_t)) && !rep) {
                        sleep(timeout);
                        cnt = 0;
                        continue;
                    }
                }
            }
            cnt += 1;
            if (cnt == HEARTBEAT_RETRY_MAX) {
                if (available_nodes & node_mask[arg->id]) {
                    debug_log_after_crash();
                    collector_fault(arg->id);
                }
                break;
            }
        }
        zmq_close(socket);
        zmq_ctx_destroy(context);
    }
    free(arg);
    return NULL;
}


void heartbeat_connect()
{
    int i;
    pthread_t thread;
    pthread_attr_t attr;

    for (i = 0; i < nr_nodes; i++) {
        if (i != node_id) {
            heartbeat_arg_t *arg;

            arg = (heartbeat_arg_t *)malloc(sizeof(heartbeat_arg_t));
            if (!arg) {
                log_err("no memory");
                return;
            }
            arg->id = i;
            tcpaddr(arg->addr, nodes[i], heartbeat_port);
            pthread_attr_init(&attr);
            pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
            pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM);
            pthread_create(&thread, &attr, heartbeat_handle, arg);
            pthread_attr_destroy(&attr);
        }
    }
}


int heartbeat_create()
{
    pthread_t thread;
    pthread_attr_t attr;
    responder_arg_t *arg;

    arg = (responder_arg_t *)calloc(1, sizeof(responder_arg_t));
    if (!arg) {
        log_err("no memeory");
        return -ENOMEM;
    }
    tcpaddr(arg->addr, inet_ntoa(get_addr()), heartbeat_port);
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM);
    pthread_create(&thread, &attr, responder_start, arg);
    pthread_attr_destroy(&attr);
    heartbeat_connect();
    return 0;
}
