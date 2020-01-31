-- dump_window_counts.sql

set schema '"feature_engineering"';

CREATE OR REPLACE FOREIGN STREAM "fe_pipeline_step_500_dump"
( "transaction_id" VARCHAR(64)
, "user_id" VARCHAR(64)
, "device_id" VARCHAR(64)
, "fname" VARCHAR(128)
, "fno" INTEGER
, "fvalue" VARCHAR(64)
, "num_dev_win_168h" BIGINT
, "denom_dev_win_168h" BIGINT
, "num_dev_win_1h" BIGINT
, "denom_dev_win_1h" BIGINT
, "num_dev_win_24h" BIGINT
, "denom_dev_win_24h" BIGINT
, "num_dev_win_6h" BIGINT
, "denom_dev_win_6h" BIGINT
, "num_dev_win_720h" BIGINT
, "denom_dev_win_720h" BIGINT
, "num_usr_win_168h" BIGINT
, "denom_usr_win_168h" BIGINT
, "num_usr_win_1h" BIGINT
, "denom_usr_win_1h" BIGINT
, "num_usr_win_24h" BIGINT
, "denom_usr_win_24h" BIGINT
, "num_usr_win_6h" BIGINT
, "denom_usr_win_6h" BIGINT
, "num_usr_win_720h" BIGINT
, "denom_usr_win_720h" BIGINT
-- velocity
, "dev_vel_168h" BIGINT
, "dev_vel_1h" BIGINT
, "dev_vel_24h" BIGINT
, "dev_vel_6h" BIGINT
, "dev_vel_720h" BIGINT
, "usr_vel_168h" BIGINT
, "usr_vel_1h" BIGINT
, "usr_vel_24h" BIGINT
, "usr_vel_6h" BIGINT
, "usr_vel_720h" BIGINT
)
SERVER FILE_SERVER
OPTIONS
( FORMATTER 'CSV'
, SEPARATOR ','
, FILE_ROTATION_SIZE '1g'
, ORIGINAL_FILENAME 'tmp-counts.csv'
, FILENAME_PREFIX 'counts'
, DIRECTORY '/home/sqlstream/output'
, WRITE_HEADER 'true'
, FILENAME_SUFFIX '.csv'
, FORMATTER_INCLUDE_ROWTIME 'false'
);

CREATE OR REPLACE PUMP "interface"."fe_pipeline_step_500_pump" STOPPED
AS
INSERT INTO "feature_engineering"."fe_pipeline_step_500_dump"
( "transaction_id"
, "user_id"
, "device_id"
, "fname"
, "fno"
, "fvalue"
, "num_dev_win_168h"
, "denom_dev_win_168h"
, "num_dev_win_1h"
, "denom_dev_win_1h"
, "num_dev_win_24h"
, "denom_dev_win_24h"
, "num_dev_win_6h"
, "denom_dev_win_6h"
, "num_dev_win_720h"
, "denom_dev_win_720h" 
, "num_usr_win_168h"
, "denom_usr_win_168h"
, "num_usr_win_1h"
, "denom_usr_win_1h"
, "num_usr_win_24h"
, "denom_usr_win_24h"
, "num_usr_win_6h"
, "denom_usr_win_6h"
, "num_usr_win_720h"
, "denom_usr_win_720h" 
-- velocity
, "dev_vel_168h"
, "dev_vel_1h"
, "dev_vel_24h"
, "dev_vel_6h"
, "dev_vel_720h" 
, "usr_vel_168h"
, "usr_vel_1h"
, "usr_vel_24h"
, "usr_vel_6h"
, "usr_vel_720h" 
)
SELECT STREAM 
  "transaction_id"
, "user_id"
, "device_id"
, "fname"
, "fno"
, "fvalue"
, "num_dev_win_168h"
, "denom_dev_win_168h"
, "num_dev_win_1h"
, "denom_dev_win_1h"
, "num_dev_win_24h"
, "denom_dev_win_24h"
, "num_dev_win_6h"
, "denom_dev_win_6h"
, "num_dev_win_720h"
, "denom_dev_win_720h" 
, "num_usr_win_168h"
, "denom_usr_win_168h"
, "num_usr_win_1h"
, "denom_usr_win_1h"
, "num_usr_win_24h"
, "denom_usr_win_24h"
, "num_usr_win_6h"
, "denom_usr_win_6h"
, "num_usr_win_720h"
, "denom_usr_win_720h" 
-- velocity
, "dev_vel_168h"
, "dev_vel_1h"
, "dev_vel_24h"
, "dev_vel_6h"
, "dev_vel_720h" 
, "usr_vel_168h"
, "usr_vel_1h"
, "usr_vel_24h"
, "usr_vel_6h"
, "usr_vel_720h" 
FROM "interface"."fe_pipeline_step_500";
