-- test_output.sql
-- 
-- Take the output from "interface"."fe_pipeline_out" and write it to a file (for comparison with the python model)

SET SCHEMA '"feature_engineering"';

CREATE OR REPLACE FOREIGN STREAM "fe_pipeline_file"
( "transaction_time" VARCHAR(24)
, "transaction_id" VARCHAR(64)
, "user_id" VARCHAR(64)
, "device_id" VARCHAR(64)
, "features_1140" VARCHAR(32000)
)
SERVER FILE_SERVER
OPTIONS
( FORMATTER 'CSV'
, SEPARATOR '|'
, FILE_ROTATION_SIZE '1g'
, ORIGINAL_FILENAME 'tmp-pipeline-out.csv'
, FILENAME_PREFIX 'pipeline_out'
, DIRECTORY '/home/sqlstream/output'
, WRITE_HEADER 'false'
, FILENAME_SUFFIX '.csv'
, FORMATTER_INCLUDE_ROWTIME 'false'
);

CREATE OR REPLACE PUMP "interface"."fe_pipeline_out_pump" STOPPED
AS
INSERT INTO "fe_pipeline_file"
( "transaction_time"
, "transaction_id" 
, "user_id" 
, "device_id" 
, "features_1140" 
)
SELECT STREAM  
  timestamp_to_char('yyyy-MM-dd HH:mm:ss',s.ROWTIME)
, "transaction_id" 
, "user_id" 
, "device_id" 
, "features_1140" 
FROM "interface"."fe_pipeline_out" s;
