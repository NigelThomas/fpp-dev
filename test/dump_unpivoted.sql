-- dump_unpivoted.sql

-- Dump out unpivoted data (for testing only) from fe step 200

set schema '"feature_engineering"';

CREATE OR REPLACE FOREIGN STREAM "fe_unpivoted_step_200"
( "transaction_id" VARCHAR(64)
, "user_id" VARCHAR(64)
, "device_id" VARCHAR(64)
, "fname" VARCHAR(128)
, "fno" INTEGER
, "fvalue" VARCHAR(64)
)
SERVER FILE_SERVER
OPTIONS
( FORMATTER 'CSV'
, SEPARATOR '|'
, FILE_ROTATION_SIZE '1g'
, ORIGINAL_FILENAME 'tmp-unpivot.csv'
, FILENAME_PREFIX 'unpivot'
, DIRECTORY '/home/sqlstream/output'
, WRITE_HEADER 'false'
, FILENAME_SUFFIX '.csv'
, FORMATTER_INCLUDE_ROWTIME 'false'
);

CREATE OR REPLACE PUMP "interface"."dump_unpivoted_pump" STOPPED
AS
INSERT INTO "fe_unpivoted_step_200"
( "transaction_id"
, "user_id"
, "device_id"
, "fname"
, "fno"
, "fvalue"
)
SELECT STREAM
  "transaction_id"
, "user_id"
, "device_id"
, "fname"
, "fno"
, "fvalue"
FROM "interface"."fe_pipeline_step_200";