-- start_matchsubs
--
-- # Ignore drop failures
-- m/.*extension \"test_planner\" does not exist/
-- s/(.*)//
--
-- # Remove all successful
-- m/INFO:  Success.*/
-- s/(.*)//
--
-- end_matchsubs

DROP EXTENSION test_planner;
CREATE EXTENSION test_planner;

SELECT test_planner();
DROP EXTENSION test_planner;
