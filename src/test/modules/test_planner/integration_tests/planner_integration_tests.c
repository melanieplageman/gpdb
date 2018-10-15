#include "postgres.h"
#include "tcop/tcopprot.h"

#include "planner_integration_tests.h"

#include "src/assertions.h"
#include "src/planner_test_helpers.h"

static void test_window_function_with_subquery_has_correct_extparams()
{
	const char *query_string = "select ( \
							       SELECT min(1) OVER() \
							       FROM pg_class \
							       WHERE relname = outer_pg_class.relname \
							   ) FROM pg_class AS outer_pg_class;";

	Query *query = make_query(query_string);
	PlannedStmt *plannedStatement = planner(query, 0, NULL);
	Plan *subplan = get_first_subplan(plannedStatement);

	assert_that_bool(bms_is_member(0, subplan->extParam), 
		is_equal_to(true));

	assert_that_bool(bms_is_member(0, subplan->allParam),
		is_equal_to(true));

	assert_that_int(list_length(plannedStatement->planTree->initPlan), 
		is_equal_to(0));

	assert_that_int(plannedStatement->nInitPlans,
		is_equal_to(0));
}

static void test_vanilla_subquery_has_correct_extparams()
{
	const char *subquery_string = "select ( \
								    select 1 from pg_class \
								    where relname = outer_pg_class.relname \
								   ) FROM pg_class as outer_pg_class;";

	Query *query = make_query(subquery_string);
	PlannedStmt *plannedStatement = planner(query, 0, NULL);
	Plan *subplan = get_first_subplan(plannedStatement);

	assert_that_bool(bms_is_member(0, subplan->extParam),
		is_equal_to(true));

	assert_that_bool(bms_is_member(0, subplan->allParam),
		is_equal_to(true));

	assert_that_int(list_length(plannedStatement->planTree->initPlan),
		is_equal_to(0));

	assert_that_int(plannedStatement->nInitPlans, 
		is_equal_to(0));
}

void run_planner_integration_test_suite(void) {
	test_vanilla_subquery_has_correct_extparams();
	test_window_function_with_subquery_has_correct_extparams();
}