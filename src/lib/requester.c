#include "requester.h"
#include "util.h"

int request(char *addr, req_t *req, rep_t *rep)
{
    int rc;
    int ret = 0;
    void *context = zmq_ctx_new();
    void *socket = zmq_socket(context, ZMQ_REQ);

    rc = zmq_connect(socket, addr);
    if (rc) {
        log_err("failed to connect");
        return -EINVAL;
    }

    rc = zmq_send(socket, req, sizeof(req_t), 0);
    if (rc != sizeof(req_t)) {
        log_err("failed to send request");
        ret = -EIO;
        goto out;
    }

    rc = zmq_recv(socket, rep, sizeof(rep_t), 0);
    if (rc != sizeof(rep_t)) {
        log_err("failed to receive reply");
        ret = -EIO;
        goto out;
    }

out:
    zmq_close(socket);
    zmq_ctx_destroy(context);
    return ret;
}
