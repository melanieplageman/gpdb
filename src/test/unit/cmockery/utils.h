#include "utils/elog.h"

/*
 * EXPECT_ELOG is a convenience macro, since these expect_any statements
 * are required and elog is a macro for elog_start and elog_finish
 */
#define EXPECT_ELOG(LOG_LEVEL)     \
	will_be_called(elog_start); \
	expect_any(elog_start, filename); \
	expect_any(elog_start, lineno); \
	expect_any(elog_start, funcname); \
	if (LOG_LEVEL < ERROR) \
	{ \
		will_be_called(elog_finish); \
	} \
	else \
	{ \
		/* In order to properly mock ERROR we must emulate the noreturn effect to exit early from the caller */ \
		will_be_called_with_sideeffect(elog_finish, &_ExceptionalCondition, NULL);\
	} \
	expect_value(elog_finish, elevel, LOG_LEVEL); \
	expect_any(elog_finish, fmt); \

#define EXPECT_EREPORT(LOG_LEVEL)     \
	expect_any(errstart, elevel); \
	expect_any(errstart, filename); \
	expect_any(errstart, lineno); \
	expect_any(errstart, funcname); \
	expect_any(errstart, domain); \
	if (LOG_LEVEL < ERROR) \
	{ \
		will_return(errstart, false); \
	} \
	else \
	{ \
		will_return_with_sideeffect(errstart, false, &_ExceptionalCondition, NULL);\
	} \

#define PG_RE_THROW() siglongjmp(*PG_exception_stack, 1)

/*
 * This method will emulate the real ExceptionalCondition
 * function by re-throwing the exception, essentially falling
 * back to the next available PG_CATCH();
 */
void
_ExceptionalCondition()
{
	PG_RE_THROW();
}

