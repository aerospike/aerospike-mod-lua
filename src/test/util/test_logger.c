#include "test_logger.h"
#include <aerospike/as_log.h>
#include "../test.h"

static bool
as_lua_log_callback(as_log_level level, const char * func, const char * file, uint32_t line, const char * fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	switch(level) {
		case AS_LOG_LEVEL_ERROR:
			atf_log_line(stderr, "ERROR", ATF_LOG_PREFIX, NULL, 0, fmt, ap);
			break;
		case AS_LOG_LEVEL_WARN:
			atf_log_line(stderr, "WARN", ATF_LOG_PREFIX, NULL, 0, fmt, ap);
			break;
		case AS_LOG_LEVEL_INFO:
			atf_log_line(stderr, "INFO", ATF_LOG_PREFIX, NULL, 0, fmt, ap);
			break;
		case AS_LOG_LEVEL_DEBUG:
			atf_log_line(stderr, "DEBUG", ATF_LOG_PREFIX, NULL, 0, fmt, ap);
			break;
		default:
			break;
	}
	va_end(ap);
	return true;
}

void
as_lua_log_init()
{
	as_log_set_level(AS_LOG_LEVEL_INFO);
	as_log_set_callback(as_lua_log_callback);
}
