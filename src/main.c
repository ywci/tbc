#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <tbc.h>
#include "collector.h"
#include "heartbeat.h"
#include "generator.h"
#include "client.h"
#include "parser.h"
#include "util.h"
#include "log.h"

int server_create()
{
#ifdef HEARTBEAT
    heartbeat_create();
    collector_create();
#endif
    return generator_create();
}


void usage(char *name)
{
    printf("Usage: %s [-c | --client] [-o filename] [-i interval]\n", name);
    printf("-c, --client     : Operate in client mode\n");
    printf("-o, --output     : Define the name of the log file\n");
    printf("-i, --interval   : Establish the evaluation interval, indicating the number of requests to be processed before generating evaluation results.\n");
}


int main(int argc, char **argv)
{
    int i = 1;
    bool enable_client = false;
    eval_intv = EVAL_INTV;
    strcpy(log_name, PATH_LOG);
    check_settings();
#ifdef FUNC_TIMER
    init_func_timer();
#endif
    while (i < argc) {
        if (!strcmp(argv[i], "-i") || !strcmp(argv[i], "--interval")) {
            i++;
            if (i == argc) {
                usage(argv[0]);
                exit(-1);
            } else {
                int n = strtol(argv[i], NULL, 0);

                if (n <= 0) {
                    usage(argv[0]);
                    exit(-1);
                }
                eval_intv = n;
            }
        } else if (!strcmp(argv[i], "-o") || !strcmp(argv[i], "--output")) {
            i++;
            if (i == argc) {
                usage(argv[0]);
                exit(-1);
            } else
                strcpy(log_name, argv[i]);
        } else if (!strcmp(argv[i], "-c") || !strcmp(argv[i], "--client"))
            enable_client = true;
        else {
            usage(argv[0]);
            exit(-1);
        }
        i++;
    }
    if (parse()) {
        log_err("failed to load configuration");
        return -1;
    }
    log_file_remove();
    if (node_id >= 0) {
        if (server_create()) {
            log_err("failed to create server");
            return -1;
        }
    }
    if (enable_client) {
        if (client_create()) {
            log_err("failed to create client");
            return -1;
        }
    }
    pause();
    return 0;
}
