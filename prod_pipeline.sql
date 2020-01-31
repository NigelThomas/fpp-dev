-- prod_pipeline.sql

-- Put together the central part of the production pipeline
-- At the beginning of the feature pipeline, data is read from the stream "interface"."transactions"
-- At the end of the scoring pipeline, data flows into the stream "interface"."scoring_pipeline_out"

-- All the pumps in this section are in the "interface" schema, so to start the pumps:
-- ALTER PUMP "interface".* START;


-- the streams used to send data into and out of this set of pipelines
!run interface.sql

-- static data used to normalize the derived features
!run normalization_data.sql

-- the feature engineering pipeline
!run feature_engineering.sql

-- the scoring pipeline
!run scoring_pipeline.sql


