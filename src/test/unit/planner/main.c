#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"


void test_planner_works(void** state)
{
	assert_int_equal(0, 1);
}

int main(int argc, char* argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
			unit_test(test_planner_works)
	};
	return run_tests(tests);
}
