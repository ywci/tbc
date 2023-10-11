#ifndef _BENCHMARK_H
#define _BENCHMARK_H

#include <zmq.h>
#include <czmq.h>
#include <time.h>
#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <net/if.h>
#include <string.h>
#include <sys/time.h>
#include <arpa/inet.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <netinet/in.h>

#define NR_PACKETS      1000
#define HIGH_WATER_MARK 1000000
#define ADDR            "ipc:///tmp/tbc"

#include "conf.h"

typedef uint32_t hid_t;
typedef struct timeval timeval_t;

#endif
