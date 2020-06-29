-- interface.sql
--
-- Create the interface table
-- This version uses a single data type VARCHAR(64) to simplify the downstream processing

CREATE OR REPLACE SCHEMA "interface";
!set force on
ALTER PUMP "interface".* stop;
!set force off

DROP SCHEMA "interface" cascade;
CREATE OR REPLACE SCHEMA "interface";

SET SCHEMA '"interface"';

-- Assumption - the data from FPP will be joined and then delivered into this interface stream

CREATE OR REPLACE STREAM "transactions"
( "transaction_id" varchar(64) 
, "tenantId" VARCHAR(256)
, "signals" VARCHAR(102400)
, "headers" VARCHAR(102400)
, "neustar" VARCHAR(102400)
, "eval" VARCHAR(102400)
, "score" VARCHAR(1024)
, "device_id" varchar(64)
, "user_id" varchar(64)
, "city_cf" varchar(8)
, "state_cf" varchar(8)
, "country_cf" varchar(8)
, "context_client" varchar(64)
, "context_events" varchar(64)
, "context_flag001" varchar(64)
, "context_flag002" varchar(64)
, "context_usergroups" varchar(64)
, "day_of_week" varchar(64)
, "gemaltoRiskEngine_attributes_deviceBrowser_browserName" varchar(64)
, "gemaltoRiskEngine_attributes_deviceBrowser_browserVersion" varchar(64)
, "gemaltoRiskEngine_attributes_deviceBrowser_isRooted" varchar(64)
, "gemaltoRiskEngine_attributes_deviceBrowser_networkIp" varchar(64)
, "gemaltoRiskEngine_attributes_deviceBrowser_osFamily" varchar(64)
, "gemaltoRiskEngine_attributes_deviceBrowser_osName" varchar(64)
, "gemaltoRiskEngine_attributes_deviceBrowser_osReleaseDateOrder" varchar(64)
, "gemaltoRiskEngine_attributes_deviceBrowser_osVersionOfFamily" varchar(64)
, "gemaltoRiskEngine_attributes_deviceBrowser_osVersion" varchar(64)
, "gemaltoRiskEngine_attributes_deviceBrowser_screenHeight" varchar(64)
, "gemaltoRiskEngine_attributes_deviceBrowser_screenWidth" varchar(64)
, "gemaltoRiskEngine_attributes_deviceBrowser_userAgent" varchar(256)
, "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_fingerprint" varchar(64)
, "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_manufacturer" varchar(64)
, "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_model" varchar(64)
, "gemaltoRiskEngine_attributes_deviceMobileApp_location_signals_source" varchar(64)
, "gemaltoRiskEngine_attributes_deviceMobileApp_network_signals_hwAddress" varchar(64)
, "gemaltoRiskEngine_attributes_deviceMobileApp_network_signals_type" varchar(64)
, "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isBluetoothEnabled" varchar(64)
, "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isLocationEnabled" varchar(64)
, "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isWifiEnabled" varchar(64)
, "gemaltoRiskEngine_attributes_deviceMobileApp_platform_signals_family" varchar(64)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_area_code" varchar(64)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_city" varchar(64)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_postal_code" varchar(64)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_time_zone" varchar(64)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country_code" varchar(64)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country" varchar(64)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state_code" varchar(64)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state" varchar(64)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_continent" varchar(64)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_dma" varchar(64)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_latitude" varchar(64)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_longitude" varchar(64)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_msa" varchar(64)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_region" varchar(64)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_Domain_sld" varchar(64)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_Domain_tld" varchar(64)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_asn" varchar(64)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_carrier" varchar(64)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_connection_type" varchar(64)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_hosting_facility" varchar(64)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_ip_routing_type" varchar(64)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_line_speed" varchar(64)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_organization" varchar(64)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ProxyData_proxy_level" varchar(64)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ProxyData_proxy_type" varchar(64)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_anonymizer_status" varchar(64)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ip_type" varchar(64)
, "hour_of_day" varchar(64)
, "visitId" varchar(64)
);

CREATE OR REPLACE STREAM "fe_pipeline_step_200"
( "transaction_id" VARCHAR(64)
, "user_id" VARCHAR(64)
, "device_id" VARCHAR(64)
, "signals" VARCHAR(102400)
, "headers" VARCHAR(102400)
, "neustar" VARCHAR(102400)
, "eval" VARCHAR(102400)
, "tenantId" VARCHAR(256)
, "score" VARCHAR(1024)
, "fname" VARCHAR(128) NOT NULL
, "fno" TINYINT NOT NULL
, "fvalue" VARCHAR(256) NOT NULL
);

-- intermediate streams for window analysis
-- 2 streams for each window - first the dev_xxx and then the usr_xxx

CREATE OR REPLACE STREAM "fe_pipeline_step_400"
( "transaction_id" VARCHAR(64)
, "user_id" VARCHAR(64)
, "device_id" VARCHAR(64)
, "tenantId" VARCHAR(256)
, "signals" VARCHAR(102400)
, "headers" VARCHAR(102400)
, "neustar" VARCHAR(102400)
, "eval" VARCHAR(102400)
, "score" VARCHAR(1024)
, "fname" VARCHAR(128) NOT NULL
, "fno" TINYINT NOT NULL
, "fvalue" VARCHAR(256) NOT NULL
, "confidence" INTEGER
--
, "num_usr_win_1h_conf" BIGINT
, "num_usr_win_1h_val" BIGINT
, "denom_win_1h_conf" BIGINT
, "denom_win_1h_val" BIGINT
, "usr_vel_1h" BIGINT
-- , "num_dev_win_1h_conf" BIGINT
-- , "num_dev_win_1h_val" BIGINT
-- , "dev_vel_1h" BIGINT
);

-- CREATE OR REPLACE STREAM "fe_pipeline_step_405"
-- ( "transaction_id" VARCHAR(64)
-- , "user_id" VARCHAR(64)
-- , "device_id" VARCHAR(64)
-- , "tenantId" VARCHAR(256)
-- , "signals" VARCHAR(102400)
-- , "headers" VARCHAR(102400)
-- , "neustar" VARCHAR(102400)
-- , "eval" VARCHAR(102400)
-- , "score" VARCHAR(1024)
-- , "fname" VARCHAR(128) NOT NULL
-- , "fno" TINYINT NOT NULL
-- , "fvalue" VARCHAR(256) NOT NULL
-- , "confidence" INTEGER
-- --
-- , "num_usr_win_1h_conf" BIGINT
-- , "num_usr_win_1h_val" BIGINT
-- , "denom_win_1h_conf" BIGINT
-- , "denom_win_1h_val" BIGINT
-- , "usr_vel_1h" BIGINT
-- , "num_dev_win_1h_conf" BIGINT
-- , "num_dev_win_1h_val" BIGINT
-- , "dev_vel_1h" BIGINT
-- );

CREATE OR REPLACE STREAM "fe_pipeline_step_410"
( "transaction_id" VARCHAR(64)
, "user_id" VARCHAR(64)
, "device_id" VARCHAR(64)
, "tenantId" VARCHAR(256)
, "signals" VARCHAR(102400)
, "headers" VARCHAR(102400)
, "neustar" VARCHAR(102400)
, "eval" VARCHAR(102400)
, "score" VARCHAR(1024)
, "fname" VARCHAR(128) NOT NULL
, "fno" TINYINT NOT NULL
, "fvalue" VARCHAR(256) NOT NULL
, "confidence" INTEGER
--
, "num_usr_win_1h_conf" BIGINT
, "num_usr_win_1h_val" BIGINT
, "denom_win_1h_conf" BIGINT
, "denom_win_1h_val" BIGINT
, "usr_vel_1h" BIGINT
-- , "num_dev_win_1h_conf" BIGINT
-- , "num_dev_win_1h_val" BIGINT
-- , "dev_vel_1h" BIGINT
--
, "num_usr_win_6h_conf" BIGINT
, "num_usr_win_6h_val" BIGINT
, "denom_win_6h_conf" BIGINT
, "denom_win_6h_val" BIGINT
, "usr_vel_6h" BIGINT
--, "num_dev_win_6h_conf" BIGINT
--, "num_dev_win_6h_val" BIGINT
--, "dev_vel_6h" BIGINT
);

-- CREATE OR REPLACE STREAM "fe_pipeline_step_415"
-- ( "transaction_id" VARCHAR(64)
-- , "user_id" VARCHAR(64)
-- , "device_id" VARCHAR(64)
-- , "tenantId" VARCHAR(256)
-- , "signals" VARCHAR(102400)
-- , "headers" VARCHAR(102400)
-- , "neustar" VARCHAR(102400)
-- , "eval" VARCHAR(102400)
-- , "score" VARCHAR(1024)
-- , "fname" VARCHAR(128) NOT NULL
-- , "fno" TINYINT NOT NULL
-- , "fvalue" VARCHAR(256) NOT NULL
-- , "confidence" INTEGER
-- --
-- , "num_usr_win_1h_conf" BIGINT
-- , "num_usr_win_1h_val" BIGINT
-- , "denom_win_1h_conf" BIGINT
-- , "denom_win_1h_val" BIGINT
-- , "usr_vel_1h" BIGINT
-- , "num_dev_win_1h_conf" BIGINT
-- , "num_dev_win_1h_val" BIGINT
-- , "dev_vel_1h" BIGINT
-- --
-- , "num_usr_win_6h_conf" BIGINT
-- , "num_usr_win_6h_val" BIGINT
-- , "denom_win_6h_conf" BIGINT
-- , "denom_win_6h_val" BIGINT
-- , "usr_vel_6h" BIGINT
-- , "num_dev_win_6h_conf" BIGINT
-- , "num_dev_win_6h_val" BIGINT
-- , "dev_vel_6h" BIGINT
-- );

CREATE OR REPLACE STREAM "fe_pipeline_step_420"
( "transaction_id" VARCHAR(64)
, "user_id" VARCHAR(64)
, "device_id" VARCHAR(64)
, "tenantId" VARCHAR(256)
, "signals" VARCHAR(102400)
, "headers" VARCHAR(102400)
, "neustar" VARCHAR(102400)
, "eval" VARCHAR(102400)
, "score" VARCHAR(1024)
, "fname" VARCHAR(128) NOT NULL
, "fno" TINYINT NOT NULL
, "fvalue" VARCHAR(256) NOT NULL
, "confidence" INTEGER
--
, "num_usr_win_1h_conf" BIGINT
, "num_usr_win_1h_val" BIGINT
, "denom_win_1h_conf" BIGINT
, "denom_win_1h_val" BIGINT
, "usr_vel_1h" BIGINT
-- , "num_dev_win_1h_conf" BIGINT
-- , "num_dev_win_1h_val" BIGINT
-- , "dev_vel_1h" BIGINT
--
, "num_usr_win_6h_conf" BIGINT
, "num_usr_win_6h_val" BIGINT
, "denom_win_6h_conf" BIGINT
, "denom_win_6h_val" BIGINT
, "usr_vel_6h" BIGINT
-- , "num_dev_win_6h_conf" BIGINT
-- , "num_dev_win_6h_val" BIGINT
-- , "dev_vel_6h" BIGINT
--
, "num_usr_win_24h_conf" BIGINT
, "num_usr_win_24h_val" BIGINT
, "denom_win_24h_conf" BIGINT
, "denom_win_24h_val" BIGINT
, "usr_vel_24h" BIGINT
--, "num_dev_win_24h_conf" BIGINT
--, "num_dev_win_24h_val" BIGINT
--, "dev_vel_24h" BIGINT
);

-- CREATE OR REPLACE STREAM "fe_pipeline_step_425"
-- ( "transaction_id" VARCHAR(64)
-- , "user_id" VARCHAR(64)
-- , "device_id" VARCHAR(64)
-- , "tenantId" VARCHAR(256)
-- , "signals" VARCHAR(102400)
-- , "headers" VARCHAR(102400)
-- , "neustar" VARCHAR(102400)
-- , "eval" VARCHAR(102400)
-- , "score" VARCHAR(1024)
-- , "fname" VARCHAR(128) NOT NULL
-- , "fno" TINYINT NOT NULL
-- , "fvalue" VARCHAR(256) NOT NULL
-- , "confidence" INTEGER
-- --
-- , "num_usr_win_1h_conf" BIGINT
-- , "num_usr_win_1h_val" BIGINT
-- , "denom_win_1h_conf" BIGINT
-- , "denom_win_1h_val" BIGINT
-- , "usr_vel_1h" BIGINT
-- , "num_dev_win_1h_conf" BIGINT
-- , "num_dev_win_1h_val" BIGINT
-- , "dev_vel_1h" BIGINT
-- --
-- , "num_usr_win_6h_conf" BIGINT
-- , "num_usr_win_6h_val" BIGINT
-- , "denom_win_6h_conf" BIGINT
-- , "denom_win_6h_val" BIGINT
-- , "usr_vel_6h" BIGINT
-- , "num_dev_win_6h_conf" BIGINT
-- , "num_dev_win_6h_val" BIGINT
-- , "dev_vel_6h" BIGINT
-- --
-- , "num_usr_win_24h_conf" BIGINT
-- , "num_usr_win_24h_val" BIGINT
-- , "denom_win_24h_conf" BIGINT
-- , "denom_win_24h_val" BIGINT
-- , "usr_vel_24h" BIGINT
-- , "num_dev_win_24h_conf" BIGINT
-- , "num_dev_win_24h_val" BIGINT
-- , "dev_vel_24h" BIGINT
-- );

CREATE OR REPLACE STREAM "fe_pipeline_step_430"
( "transaction_id" VARCHAR(64)
, "user_id" VARCHAR(64)
, "device_id" VARCHAR(64)
, "tenantId" VARCHAR(256)
, "signals" VARCHAR(102400)
, "headers" VARCHAR(102400)
, "neustar" VARCHAR(102400)
, "eval" VARCHAR(102400)
, "score" VARCHAR(1024)
, "fname" VARCHAR(128) NOT NULL
, "fno" TINYINT NOT NULL
, "fvalue" VARCHAR(256) NOT NULL
, "confidence" INTEGER
--
, "num_usr_win_1h_conf" BIGINT
, "num_usr_win_1h_val" BIGINT
, "denom_win_1h_conf" BIGINT
, "denom_win_1h_val" BIGINT
, "usr_vel_1h" BIGINT
-- , "num_dev_win_1h_conf" BIGINT
-- , "num_dev_win_1h_val" BIGINT
-- , "dev_vel_1h" BIGINT
--
, "num_usr_win_6h_conf" BIGINT
, "num_usr_win_6h_val" BIGINT
, "denom_win_6h_conf" BIGINT
, "denom_win_6h_val" BIGINT
, "usr_vel_6h" BIGINT
-- , "num_dev_win_6h_conf" BIGINT
-- , "num_dev_win_6h_val" BIGINT
-- , "dev_vel_6h" BIGINT
--
, "num_usr_win_24h_conf" BIGINT
, "num_usr_win_24h_val" BIGINT
, "denom_win_24h_conf" BIGINT
, "denom_win_24h_val" BIGINT
, "usr_vel_24h" BIGINT
-- , "num_dev_win_24h_conf" BIGINT
-- , "num_dev_win_24h_val" BIGINT
-- , "dev_vel_24h" BIGINT
--
, "num_usr_win_168h_conf" BIGINT
, "num_usr_win_168h_val" BIGINT
, "denom_win_168h_conf" BIGINT
, "denom_win_168h_val" BIGINT
, "usr_vel_168h" BIGINT
--, "num_dev_win_168h_conf" BIGINT
--, "num_dev_win_168h_val" BIGINT
--, "dev_vel_168h" BIGINT
);

-- CREATE OR REPLACE STREAM "fe_pipeline_step_435"
-- ( "transaction_id" VARCHAR(64)
-- , "user_id" VARCHAR(64)
-- , "device_id" VARCHAR(64)
-- , "tenantId" VARCHAR(256)
-- , "signals" VARCHAR(102400)
-- , "headers" VARCHAR(102400)
-- , "neustar" VARCHAR(102400)
-- , "eval" VARCHAR(102400)
-- , "score" VARCHAR(1024)
-- , "fname" VARCHAR(128) NOT NULL
-- , "fno" TINYINT NOT NULL
-- , "fvalue" VARCHAR(256) NOT NULL
-- , "confidence" INTEGER
-- --
-- , "num_usr_win_1h_conf" BIGINT
-- , "num_usr_win_1h_val" BIGINT
-- , "denom_win_1h_conf" BIGINT
-- , "denom_win_1h_val" BIGINT
-- , "usr_vel_1h" BIGINT
-- , "num_dev_win_1h_conf" BIGINT
-- , "num_dev_win_1h_val" BIGINT
-- , "dev_vel_1h" BIGINT
-- --
-- , "num_usr_win_6h_conf" BIGINT
-- , "num_usr_win_6h_val" BIGINT
-- , "denom_win_6h_conf" BIGINT
-- , "denom_win_6h_val" BIGINT
-- , "usr_vel_6h" BIGINT
-- , "num_dev_win_6h_conf" BIGINT
-- , "num_dev_win_6h_val" BIGINT
-- , "dev_vel_6h" BIGINT
-- --
-- , "num_usr_win_24h_conf" BIGINT
-- , "num_usr_win_24h_val" BIGINT
-- , "denom_win_24h_conf" BIGINT
-- , "denom_win_24h_val" BIGINT
-- , "usr_vel_24h" BIGINT
-- , "num_dev_win_24h_conf" BIGINT
-- , "num_dev_win_24h_val" BIGINT
-- , "dev_vel_24h" BIGINT
-- --
-- , "num_usr_win_168h_conf" BIGINT
-- , "num_usr_win_168h_val" BIGINT
-- , "denom_win_168h_conf" BIGINT
-- , "denom_win_168h_val" BIGINT
-- , "usr_vel_168h" BIGINT
-- , "num_dev_win_168h_conf" BIGINT
-- , "num_dev_win_168h_val" BIGINT
-- , "dev_vel_168h" BIGINT
-- );

CREATE OR REPLACE STREAM "fe_pipeline_step_440"
( "transaction_id" VARCHAR(64)
, "user_id" VARCHAR(64)
, "device_id" VARCHAR(64)
, "tenantId" VARCHAR(256)
, "signals" VARCHAR(102400)
, "headers" VARCHAR(102400)
, "neustar" VARCHAR(102400)
, "eval" VARCHAR(102400)
, "score" VARCHAR(1024)
, "fname" VARCHAR(128) NOT NULL
, "fno" TINYINT NOT NULL
, "fvalue" VARCHAR(256) NOT NULL
, "confidence" INTEGER
--
, "num_usr_win_1h_conf" BIGINT
, "num_usr_win_1h_val" BIGINT
, "denom_win_1h_conf" BIGINT
, "denom_win_1h_val" BIGINT
, "usr_vel_1h" BIGINT
-- , "num_dev_win_1h_conf" BIGINT
-- , "num_dev_win_1h_val" BIGINT
-- , "dev_vel_1h" BIGINT
--
, "num_usr_win_6h_conf" BIGINT
, "num_usr_win_6h_val" BIGINT
, "denom_win_6h_conf" BIGINT
, "denom_win_6h_val" BIGINT
, "usr_vel_6h" BIGINT
-- , "num_dev_win_6h_conf" BIGINT
-- , "num_dev_win_6h_val" BIGINT
-- , "dev_vel_6h" BIGINT
--
, "num_usr_win_24h_conf" BIGINT
, "num_usr_win_24h_val" BIGINT
, "denom_win_24h_conf" BIGINT
, "denom_win_24h_val" BIGINT
, "usr_vel_24h" BIGINT
-- , "num_dev_win_24h_conf" BIGINT
-- , "num_dev_win_24h_val" BIGINT
-- , "dev_vel_24h" BIGINT
--
, "num_usr_win_168h_conf" BIGINT
, "num_usr_win_168h_val" BIGINT
, "denom_win_168h_conf" BIGINT
, "denom_win_168h_val" BIGINT
, "usr_vel_168h" BIGINT
-- , "num_dev_win_168h_conf" BIGINT
-- , "num_dev_win_168h_val" BIGINT
-- , "dev_vel_168h" BIGINT
--
, "num_usr_win_720h_conf" BIGINT
, "num_usr_win_720h_val" BIGINT
, "denom_win_720h_conf" BIGINT
, "denom_win_720h_val" BIGINT
, "usr_vel_720h" BIGINT
--, "num_dev_win_720h_conf" BIGINT
--, "num_dev_win_720h_val" BIGINT
--, "dev_vel_720h" BIGINT
);

-- CREATE OR REPLACE STREAM "fe_pipeline_step_500"
-- ( "transaction_id" VARCHAR(64)
-- , "user_id" VARCHAR(64)
-- , "device_id" VARCHAR(64)
-- , "tenantId" VARCHAR(256)
-- , "signals" VARCHAR(102400)
-- , "headers" VARCHAR(102400)
-- , "neustar" VARCHAR(102400)
-- , "eval" VARCHAR(102400)
-- , "score" VARCHAR(1024)
-- , "fname" VARCHAR(128) NOT NULL
-- , "fno" TINYINT NOT NULL
-- , "fvalue" VARCHAR(256) NOT NULL
-- , "confidence" INTEGER
-- --
-- , "num_usr_win_1h_conf" BIGINT
-- , "num_usr_win_1h_val" BIGINT
-- , "denom_win_1h_conf" BIGINT
-- , "denom_win_1h_val" BIGINT
-- , "usr_vel_1h" BIGINT
-- , "num_dev_win_1h_conf" BIGINT
-- , "num_dev_win_1h_val" BIGINT
-- , "dev_vel_1h" BIGINT
-- --
-- , "num_usr_win_6h_conf" BIGINT
-- , "num_usr_win_6h_val" BIGINT
-- , "denom_win_6h_conf" BIGINT
-- , "denom_win_6h_val" BIGINT
-- , "usr_vel_6h" BIGINT
-- , "num_dev_win_6h_conf" BIGINT
-- , "num_dev_win_6h_val" BIGINT
-- , "dev_vel_6h" BIGINT
-- --
-- , "num_usr_win_24h_conf" BIGINT
-- , "num_usr_win_24h_val" BIGINT
-- , "denom_win_24h_conf" BIGINT
-- , "denom_win_24h_val" BIGINT
-- , "usr_vel_24h" BIGINT
-- , "num_dev_win_24h_conf" BIGINT
-- , "num_dev_win_24h_val" BIGINT
-- , "dev_vel_24h" BIGINT
-- --
-- , "num_usr_win_168h_conf" BIGINT
-- , "num_usr_win_168h_val" BIGINT
-- , "denom_win_168h_conf" BIGINT
-- , "denom_win_168h_val" BIGINT
-- , "usr_vel_168h" BIGINT
-- , "num_dev_win_168h_conf" BIGINT
-- , "num_dev_win_168h_val" BIGINT
-- , "dev_vel_168h" BIGINT
-- --
-- , "num_usr_win_720h_conf" BIGINT
-- , "num_usr_win_720h_val" BIGINT
-- , "denom_win_720h_conf" BIGINT
-- , "denom_win_720h_val" BIGINT
-- , "usr_vel_720h" BIGINT
-- , "num_dev_win_720h_conf" BIGINT
-- , "num_dev_win_720h_val" BIGINT
-- , "dev_vel_720h" BIGINT
-- );

-- The feature engineering pipeline will deliver data to this stream

CREATE OR REPLACE STREAM "interface"."fe_pipeline_out"
( "transaction_id" VARCHAR(64)
, "user_id" VARCHAR(64)
, "device_id" VARCHAR(64)
, "tenantId" VARCHAR(256)
, "signals" VARCHAR(102400)
, "headers" VARCHAR(102400)
, "neustar" VARCHAR(102400)
, "eval" VARCHAR(102400)
, "score" VARCHAR(1024)
, "features_1140" VARCHAR(32000)
);

-- the scoring pipeline delivers data to this stream

CREATE OR REPLACE STREAM "interface"."scoring_pipeline_out"
("transaction_id"       VARCHAR(64)
, "user_id"             VARCHAR(64)
, "device_id"           VARCHAR(64)
, "tenantId" VARCHAR(256)
, "signals" VARCHAR(102400)
, "headers" VARCHAR(102400)
, "neustar" VARCHAR(102400)
, "eval" VARCHAR(102400)
, "score" VARCHAR(1024)
, "post_data"           VARCHAR(32000)
, "httpResult"          INTEGER           -- the HTTP status
, "httpResponse"        VARCHAR(1000)     -- this is the JSON response
, "result"              REAL              -- the score (extracted from JSON response)
, "error"               VARCHAR(250)      -- remaining elements extracted from JSON response
, "preProcessing"       BIGINT
, "wait"                BIGINT 
, "execution"           BIGINT
, "functionInternal"    BIGINT 
);