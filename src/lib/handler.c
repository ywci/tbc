#include "handler.h"
#include "callback.h"
#include "evaluator.h"

void handle(char *buf, size_t size)
{
#ifdef EVALUATE
    evaluate(buf, size);
#endif
    callback(buf, size);
}
