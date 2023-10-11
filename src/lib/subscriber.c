#include "subscriber.h"
#include "util.h"

void *subscriber_start(void *ptr)
{
    int ret;
    void *context;
    void *backend;
    void *frontend;
    sub_arg_t *arg = (sub_arg_t *)ptr;
#ifdef HIGH_WATER_MARK
    int hwm = HIGH_WATER_MARK;
#endif

    if (!arg) {
        log_err("invalid argumuent");
        return NULL;
    }

    log_func("src=%s, dest=%s", arg->src, arg->dest);
    context = zmq_ctx_new();
    frontend = zmq_socket(context, ZMQ_SUB);
    backend = zmq_socket(context, ZMQ_PUSH);
#ifdef HIGH_WATER_MARK
    zmq_setsockopt(frontend, ZMQ_RCVHWM, &hwm, sizeof(hwm));
    zmq_setsockopt(backend, ZMQ_SNDHWM, &hwm, sizeof(hwm));
#endif
    ret = zmq_connect(frontend, arg->src);
    if (!ret) {
        if (zmq_setsockopt(frontend, ZMQ_SUBSCRIBE, "", 0)) {
            log_err("failed to set socket");
            assert(0);
        }
        ret = zmq_connect(backend, arg->dest);
        if (!ret)
            forward(frontend, backend, NULL, NULL);
    }

    if (ret)
        log_err("failed to start subscriber");

    zmq_close(frontend);
    zmq_close(backend);
    zmq_ctx_destroy(context);

    free(arg);
    return NULL;
}
