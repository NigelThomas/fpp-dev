-- test_pipeline.sql
--
-- Put together the test pipeline
!run test/reset_pipeline.sql

!run interface.sql

-- static data
!run normalization_data.sql

-- use the test JSON ingest from file
!run test/ingest_json.sql


!run feature_engineering.sql
--!run scoring_pipeline.sql

!run test/test_output.sql
-- !run test/dump_input.sql
-- !run test/dump_window_counts.sql

-- now create the ingest pump needed for testing
!run test/cast_ingest_pump.sql

-- pumps will be started by the image launch
