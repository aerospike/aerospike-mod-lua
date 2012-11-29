#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>

void log_append(const char * file, int line, const char * fmt, ...) {
    char msg[128] = {0};
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, 128, fmt, ap);
    va_end(ap);
    printf("%s:%d – %s\n",file,line,msg);
}

#define log(fmt, args...) \
    // log_append(__FILE__, __LINE__, fmt, ## args);
