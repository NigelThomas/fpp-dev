-- model_data.sql
--
-- A foreign stream to read the fpp_normalise_min_max.txt data

CREATE OR REPLACE SCHEMA "feature_engineering";

SET SCHEMA '"feature_engineering"';

CREATE OR REPLACE FOREIGN TABLE "normalization_parameters"
( "fname"  VARCHAR(128)
, "min"   DOUBLE
, "max" DOUBLE
)
SERVER FILE_SERVER
OPTIONS 
( PARSER 'CSV'
, DIRECTORY '/home/sqlstream/fpp-dev/data'
, FILENAME_PATTERN 'fpp_normalize_min_max\.txt'
, SEPARATOR ','
, STATIC_FILES 'true'
, SKIP_HEADER 'false'
);

-- we want to take names like visitId_dev_vel_6h and split at the 3rd underscore from the (right hand) end
-- we could just change the source data file, but to avoid confusion do it here 


CREATE OR REPLACE VIEW "fe_normalization_step_1"
AS
SELECT * 
     , regex_replace("fname",'(_[^_]+){3}$','',1,0) as "raw_fname"
FROM "normalization_parameters";

CREATE OR REPLACE VIEW "fe_normalization_step_2"
AS
SELECT * 
     , regex_replace("fname" ,"raw_fname"||'_','',1,0) as "window_name"
FROM "fe_normalization_step_1";

-- Pivot to one row per raw feature (so we can join to the fe_pipeline)

CREATE OR REPLACE VIEW "fe_normalization_step_3"
AS 
SELECT "raw_fname"
-- dev_vel_168h
, MAX(CASE WHEN "window_name" = 'dev_vel_168h' THEN "min" ELSE NULL END) as "min_dev_vel_168h"
, MAX(CASE WHEN "window_name" = 'dev_vel_168h' THEN "max" ELSE NULL END) as "max_dev_vel_168h"
-- dev_vel_1h
, MAX(CASE WHEN "window_name" = 'dev_vel_1h' THEN "min" ELSE NULL END) as "min_dev_vel_1h"
, MAX(CASE WHEN "window_name" = 'dev_vel_1h' THEN "max" ELSE NULL END) as "max_dev_vel_1h"
-- dev_vel_24h
, MAX(CASE WHEN "window_name" = 'dev_vel_24h' THEN "min" ELSE NULL END) as "min_dev_vel_24h"
, MAX(CASE WHEN "window_name" = 'dev_vel_24h' THEN "max" ELSE NULL END) as "max_dev_vel_24h"
-- dev_vel_6h
, MAX(CASE WHEN "window_name" = 'dev_vel_6h' THEN "min" ELSE NULL END) as "min_dev_vel_6h"
, MAX(CASE WHEN "window_name" = 'dev_vel_6h' THEN "max" ELSE NULL END) as "max_dev_vel_6h"
-- dev_vel_720h
, MAX(CASE WHEN "window_name" = 'dev_vel_720h' THEN "min" ELSE NULL END) as "min_dev_vel_720h"
, MAX(CASE WHEN "window_name" = 'dev_vel_720h' THEN "max" ELSE NULL END) as "max_dev_vel_720h"
-- dev_win_168h
, MAX(CASE WHEN "window_name" = 'dev_win_168h' THEN "min" ELSE NULL END) as "min_dev_win_168h"
, MAX(CASE WHEN "window_name" = 'dev_win_168h' THEN "max" ELSE NULL END) as "max_dev_win_168h"
-- dev_win_1h
, MAX(CASE WHEN "window_name" = 'dev_win_1h' THEN "min" ELSE NULL END) as "min_dev_win_1h"
, MAX(CASE WHEN "window_name" = 'dev_win_1h' THEN "max" ELSE NULL END) as "max_dev_win_1h"
-- dev_win_24h
, MAX(CASE WHEN "window_name" = 'dev_win_24h' THEN "min" ELSE NULL END) as "min_dev_win_24h"
, MAX(CASE WHEN "window_name" = 'dev_win_24h' THEN "max" ELSE NULL END) as "max_dev_win_24h"
-- dev_win_6h
, MAX(CASE WHEN "window_name" = 'dev_win_6h' THEN "min" ELSE NULL END) as "min_dev_win_6h"
, MAX(CASE WHEN "window_name" = 'dev_win_6h' THEN "max" ELSE NULL END) as "max_dev_win_6h"
-- dev_win_720h
, MAX(CASE WHEN "window_name" = 'dev_win_720h' THEN "min" ELSE NULL END) as "min_dev_win_720h"
, MAX(CASE WHEN "window_name" = 'dev_win_720h' THEN "max" ELSE NULL END) as "max_dev_win_720h"
-- usr_vel_168h
, MAX(CASE WHEN "window_name" = 'usr_vel_168h' THEN "min" ELSE NULL END) as "min_usr_vel_168h"
, MAX(CASE WHEN "window_name" = 'usr_vel_168h' THEN "max" ELSE NULL END) as "max_usr_vel_168h"
-- usr_vel_1h
, MAX(CASE WHEN "window_name" = 'usr_vel_1h' THEN "min" ELSE NULL END) as "min_usr_vel_1h"
, MAX(CASE WHEN "window_name" = 'usr_vel_1h' THEN "max" ELSE NULL END) as "max_usr_vel_1h"
-- usr_vel_24h
, MAX(CASE WHEN "window_name" = 'usr_vel_24h' THEN "min" ELSE NULL END) as "min_usr_vel_24h"
, MAX(CASE WHEN "window_name" = 'usr_vel_24h' THEN "max" ELSE NULL END) as "max_usr_vel_24h"
-- usr_vel_6h
, MAX(CASE WHEN "window_name" = 'usr_vel_6h' THEN "min" ELSE NULL END) as "min_usr_vel_6h"
, MAX(CASE WHEN "window_name" = 'usr_vel_6h' THEN "max" ELSE NULL END) as "max_usr_vel_6h"
-- usr_vel_720h
, MAX(CASE WHEN "window_name" = 'usr_vel_720h' THEN "min" ELSE NULL END) as "min_usr_vel_720h"
, MAX(CASE WHEN "window_name" = 'usr_vel_720h' THEN "max" ELSE NULL END) as "max_usr_vel_720h"
-- usr_win_168h
, MAX(CASE WHEN "window_name" = 'usr_win_168h' THEN "min" ELSE NULL END) as "min_usr_win_168h"
, MAX(CASE WHEN "window_name" = 'usr_win_168h' THEN "max" ELSE NULL END) as "max_usr_win_168h"
-- usr_win_1h
, MAX(CASE WHEN "window_name" = 'usr_win_1h' THEN "min" ELSE NULL END) as "min_usr_win_1h"
, MAX(CASE WHEN "window_name" = 'usr_win_1h' THEN "max" ELSE NULL END) as "max_usr_win_1h"
-- usr_win_24h
, MAX(CASE WHEN "window_name" = 'usr_win_24h' THEN "min" ELSE NULL END) as "min_usr_win_24h"
, MAX(CASE WHEN "window_name" = 'usr_win_24h' THEN "max" ELSE NULL END) as "max_usr_win_24h"
-- usr_win_6h
, MAX(CASE WHEN "window_name" = 'usr_win_6h' THEN "min" ELSE NULL END) as "min_usr_win_6h"
, MAX(CASE WHEN "window_name" = 'usr_win_6h' THEN "max" ELSE NULL END) as "max_usr_win_6h"
-- usr_win_720h
, MAX(CASE WHEN "window_name" = 'usr_win_720h' THEN "min" ELSE NULL END) as "min_usr_win_720h"
, MAX(CASE WHEN "window_name" = 'usr_win_720h' THEN "max" ELSE NULL END) as "max_usr_win_720h"
FROM "fe_normalization_step_2"
GROUP BY "raw_fname";

CREATE OR REPLACE VIEW "fe_normalization_flattened"
AS 
SELECT * 
FROM "fe_normalization_step_3";
