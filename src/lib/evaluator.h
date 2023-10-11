#ifndef _EVALUATOR_H
#define _EVALUATOR_H

#include "util.h"

// #define EVAL_ECHO
// #define EVAL_LATENCY
#define EVAL_THROUGHPUT

#if defined(EVAL_ECHO) || defined(EVAL_LATENCY) || defined(EVAL_THROUGHPUT)
#define EVALUATE
#endif

#define get_evaluator(hid) (hid % nr_nodes)

void eval_create();
void evaluate(char *buf, size_t size);

#endif
