#include "responder.h"

void *responder_start(void *ptr)
{
    int ret;
    void *socket;
    void *context;
    responder_t callback;
    responder_arg_t *arg = (responder_arg_t *)ptr;

    if (!arg) {
        log_err("invalid argument");
        return NULL;
    }

    callback = arg->responder;
    context = zmq_ctx_new();
    socket = zmq_socket(context, ZMQ_REP);
    ret = zmq_bind(socket, arg->addr);
    free(arg);
    if (ret) {
        log_err("failed to start responder, addr=%s", arg->addr);
        return NULL;
    }

    while (true) {
        req_t req;
        rep_t rep;

        zmq_recv(socket, &req, sizeof(req_t), 0);
        if (callback)
            rep = callback(req);
        else
            memset(&rep, 0, sizeof(rep_t));
        zmq_send(socket, &rep, sizeof(rep_t), 0);
    }

    return NULL;
}
