#ifndef FORMATTER_H
#define FORMATTER_H
#include <assert.h>

#define yes true
#define no false

int argTypeFrmtr(bool input, bool output, bool array, unsigned int type, unsigned int length = 0) {
    assert(!array || (array && length>0));
    unsigned int code = 0;
    if (input) code = 1u << ARG_INPUT;
    if (output) code |= (1u << ARG_OUTPUT);
    if (array) code |= ((1u << ARG_ARRAY) | length);
    return (code | (type << 16u));
}

#endif
