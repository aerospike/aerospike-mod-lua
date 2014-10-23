#include "test_logger.h"
#include <aerospike/as_log.h>
#include "../test.h"

static bool
as_lua_log_callback(as_log_level level, const char * func, const char * file, uint32_t line, const char * fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	atf_log_line(stderr, as_log_level_tostring(level), ATF_LOG_PREFIX, NULL, 0, fmt, ap);
	va_end(ap);
	return true;
}

void
as_lua_log_init()
{
	as_log_set_level(AS_LOG_LEVEL_INFO);
	as_log_set_callback(as_lua_log_callback);
}
