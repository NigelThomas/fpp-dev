-- features.sql
-- the feature engineering pipeline drives from interface.transactions
--
--
create or replace schema "feature_engineering";
set schema '"feature_engineering"';
set path '"feature_engineering"';

-- Replace any nulls with 'NULL'
-- except for _cf fields replace with 50 (TODO - check)


create or replace view "fe_pipeline_step_010"
as
select stream
  "transaction_id"
, "tenantId"
, "signals" 
, "headers" 
, "neustar" 
, "eval" 
, "score" 
, coalesce("user_id",'NULL') as "user_id"
, coalesce("device_id",'NULL') as "device_id"
, coalesce("city_cf",'50') as "city_cf"
, coalesce("state_cf",'50') as "state_cf"
, coalesce("country_cf",'50') as "country_cf"
, coalesce("context_client",'NULL') as "context_client" 
, coalesce("context_events",'NULL') as "context_events"
, coalesce("context_flag001",'NULL') as "context_flag001"
, coalesce("context_flag002",'NULL') as "context_flag002"
, coalesce("context_usergroups",'NULL') as "context_usergroups" 
, coalesce("day_of_week",'8') as "day_of_week" 
, coalesce("gemaltoRiskEngine_attributes_deviceBrowser_browserName",'NULL') as "gemaltoRiskEngine_attributes_deviceBrowser_browserName"
, coalesce("gemaltoRiskEngine_attributes_deviceBrowser_browserVersion",'NULL') as "gemaltoRiskEngine_attributes_deviceBrowser_browserVersion"
, coalesce( "gemaltoRiskEngine_attributes_deviceBrowser_isRooted",'NULL') AS  "gemaltoRiskEngine_attributes_deviceBrowser_isRooted" 
, coalesce( "gemaltoRiskEngine_attributes_deviceBrowser_networkIp",'NULL') AS  "gemaltoRiskEngine_attributes_deviceBrowser_networkIp" 
, coalesce( "gemaltoRiskEngine_attributes_deviceBrowser_osFamily",'NULL') AS  "gemaltoRiskEngine_attributes_deviceBrowser_osFamily" 
, coalesce( "gemaltoRiskEngine_attributes_deviceBrowser_osName",'NULL') AS  "gemaltoRiskEngine_attributes_deviceBrowser_osName" 
, coalesce( "gemaltoRiskEngine_attributes_deviceBrowser_osReleaseDateOrder",'NULL') AS  "gemaltoRiskEngine_attributes_deviceBrowser_osReleaseDateOrder" 
, coalesce( "gemaltoRiskEngine_attributes_deviceBrowser_osVersionOfFamily",'NULL') AS  "gemaltoRiskEngine_attributes_deviceBrowser_osVersionOfFamily" 
, coalesce( "gemaltoRiskEngine_attributes_deviceBrowser_osVersion",'NULL') AS  "gemaltoRiskEngine_attributes_deviceBrowser_osVersion" 
, coalesce( "gemaltoRiskEngine_attributes_deviceBrowser_screenHeight",'NULL') AS  "gemaltoRiskEngine_attributes_deviceBrowser_screenHeight" 
, coalesce( "gemaltoRiskEngine_attributes_deviceBrowser_screenWidth",'NULL') AS  "gemaltoRiskEngine_attributes_deviceBrowser_screenWidth" 
, coalesce( "gemaltoRiskEngine_attributes_deviceBrowser_userAgent",'NULL') AS  "gemaltoRiskEngine_attributes_deviceBrowser_userAgent" 
, coalesce( "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_fingerprint",'NULL') AS  "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_fingerprint" 
, coalesce( "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_manufacturer",'NULL') AS  "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_manufacturer" 
, coalesce( "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_model",'NULL') AS  "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_model" 
, coalesce( "gemaltoRiskEngine_attributes_deviceMobileApp_location_signals_source",'NULL') AS  "gemaltoRiskEngine_attributes_deviceMobileApp_location_signals_source" 
, coalesce( "gemaltoRiskEngine_attributes_deviceMobileApp_network_signals_hwAddress",'NULL') AS  "gemaltoRiskEngine_attributes_deviceMobileApp_network_signals_hwAddress" 
, coalesce( "gemaltoRiskEngine_attributes_deviceMobileApp_network_signals_type",'NULL') AS  "gemaltoRiskEngine_attributes_deviceMobileApp_network_signals_type" 
, coalesce( "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isBluetoothEnabled",'NULL') AS  "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isBluetoothEnabled" 
, coalesce( "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isLocationEnabled",'NULL') AS  "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isLocationEnabled" 
, coalesce( "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isWifiEnabled",'NULL') AS  "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isWifiEnabled" 
, coalesce( "gemaltoRiskEngine_attributes_deviceMobileApp_platform_signals_family",'NULL') AS  "gemaltoRiskEngine_attributes_deviceMobileApp_platform_signals_family" 
, coalesce( "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_area_code",'NULL') AS  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_area_code" 
, coalesce( "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_city",'NULL') AS  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_city" 
, coalesce( "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_postal_code",'NULL') AS  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_postal_code" 
, coalesce( "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_time_zone",'NULL') AS  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_time_zone" 
, coalesce( "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country_code",'NULL') AS  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country_code" 
, coalesce( "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country",'NULL') AS  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country" 
, coalesce( "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state_code",'NULL') AS  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state_code" 
, coalesce( "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state",'NULL') AS  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state"  
, coalesce( "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_continent",'NULL') AS  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_continent" 
, coalesce( "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_dma",'NULL') AS  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_dma" 
, coalesce( "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_latitude",'NULL') AS  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_latitude" 
, coalesce( "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_longitude",'NULL') AS  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_longitude" 
, coalesce( "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_msa",'NULL') AS  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_msa" 
, coalesce( "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_region",'NULL') AS  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_region" 
, coalesce( "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_Domain_sld",'NULL') AS  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_Domain_sld" 
, coalesce( "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_Domain_tld",'NULL') AS  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_Domain_tld" 
, coalesce( "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_asn",'NULL') AS  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_asn" 
, coalesce( "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_carrier",'NULL') AS  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_carrier" 
, coalesce( "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_connection_type",'NULL') AS  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_connection_type" 
, coalesce( "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_hosting_facility",'NULL') AS  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_hosting_facility" 
, coalesce( "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_ip_routing_type",'NULL') AS  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_ip_routing_type" 
, coalesce( "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_line_speed",'NULL') AS  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_line_speed" 
, coalesce( "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_organization",'NULL') AS  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_organization" 
, coalesce( "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ProxyData_proxy_level",'NULL') AS  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ProxyData_proxy_level" 
, coalesce( "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ProxyData_proxy_type",'NULL') AS  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ProxyData_proxy_type" 
, coalesce( "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_anonymizer_status",'NULL') AS  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_anonymizer_status" 
, coalesce( "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ip_type",'NULL') AS  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ip_type" 
, coalesce("hour_of_day",'-1') as "hour_of_day"
, coalesce("visitId",'NULL') AS  "visitId"
FROM "interface"."transactions";

-- combine confidence with locations city, state, country prior to unpivot

create or replace view "fe_pipeline_step_100"
as
select stream
  "transaction_id" 
, "tenantId"
, "signals" 
, "headers" 
, "neustar" 
, "eval" 
, "score" 
, "user_id" 
, "device_id" 
, "context_client" 
, "context_events" 
, "context_flag001" 
, "context_flag002" 
, "context_usergroups" 
, "day_of_week" 
, "gemaltoRiskEngine_attributes_deviceBrowser_browserName" 
, "gemaltoRiskEngine_attributes_deviceBrowser_browserVersion" 
, "gemaltoRiskEngine_attributes_deviceBrowser_isRooted" 
, "gemaltoRiskEngine_attributes_deviceBrowser_networkIp" 
, "gemaltoRiskEngine_attributes_deviceBrowser_osFamily" 
, "gemaltoRiskEngine_attributes_deviceBrowser_osName" 
, "gemaltoRiskEngine_attributes_deviceBrowser_osReleaseDateOrder" 
, "gemaltoRiskEngine_attributes_deviceBrowser_osVersionOfFamily" 
, "gemaltoRiskEngine_attributes_deviceBrowser_osVersion" 
, "gemaltoRiskEngine_attributes_deviceBrowser_screenHeight" 
, "gemaltoRiskEngine_attributes_deviceBrowser_screenWidth" 
, "gemaltoRiskEngine_attributes_deviceBrowser_userAgent" 
, "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_fingerprint" 
, "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_manufacturer" 
, "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_model" 
, "gemaltoRiskEngine_attributes_deviceMobileApp_location_signals_source" 
, "gemaltoRiskEngine_attributes_deviceMobileApp_network_signals_hwAddress" 
, "gemaltoRiskEngine_attributes_deviceMobileApp_network_signals_type" 
, "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isBluetoothEnabled" 
, "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isLocationEnabled" 
, "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isWifiEnabled" 
, "gemaltoRiskEngine_attributes_deviceMobileApp_platform_signals_family" 
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_area_code" 
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_city" || ':' ||CAST("city_cf" AS VARCHAR(8)) as "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_city"
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_postal_code" 
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_time_zone" 
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country_code" 
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country" || ':' || CAST("country_cf" AS VARCHAR(8)) as "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country"
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state_code" 
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state"  || ':' || CAST("state_cf" AS VARCHAR(8)) as "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state"
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_continent" 
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_dma" 
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_latitude" 
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_longitude" 
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_msa" 
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_region" 
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_Domain_sld" 
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_Domain_tld" 
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_asn" 
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_carrier" 
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_connection_type" 
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_hosting_facility" 
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_ip_routing_type" 
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_line_speed" 
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_organization" 
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ProxyData_proxy_level" 
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ProxyData_proxy_type" 
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_anonymizer_status" 
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ip_type" 
, "hour_of_day" 
, "visitId" 
FROM "feature_engineering"."fe_pipeline_step_010";


!set force on
DROP FUNCTION "unpivot_features" CASCADE;
DROP FUNCTION "pivot_features" CASCADE;
DROP JAR PIVOT OPTIONS(0);
!set force off

create or replace jar pivot
    LIBRARY 'file:/home/sqlstream/fpp-dev/Pivot.jar'
    OPTIONS(0);


CREATE OR REPLACE FUNCTION "unpivot_features"
( "inputRows" CURSOR
, "unpivotColumnCount" INTEGER
, "keyColumnName" VARCHAR(128)
, "valueColumnName" VARCHAR(128)
, "indexColumnName" VARCHAR(128)
) RETURNS TABLE
( ROWTIME TIMESTAMP
, "transaction_id" VARCHAR(64)
, "tenantId" VARCHAR(256)
, "signals" VARCHAR(102400)
, "headers" VARCHAR(102400)
, "neustar" VARCHAR(102400)
, "eval" VARCHAR(102400)
, "score" VARCHAR(1024)
, "user_id" VARCHAR(64)
, "device_id" VARCHAR(64)
, "fname" VARCHAR(128)
, "fno" INTEGER
, "fvalue" VARCHAR(64)
)
LANGUAGE JAVA
PARAMETER STYLE SYSTEM DEFINED JAVA
NO SQL
EXTERNAL NAME 'PIVOT:com.sqlstream.udx.UnpivotUdx.unpivotByCount';

-- 200: unpivot so we have one feature per row

CREATE OR REPLACE VIEW "fe_pipeline_step_200"
AS SELECT STREAM *
FROM STREAM
( "feature_engineering"."unpivot_features"
	( CURSOR(SELECT STREAM * FROM "fe_pipeline_step_100")
	, 57
	, 'fname'
	, 'fno'
	, 'fvalue'
	)
);

-- materialize step 200 into an interface stream that we can dump for testing
-- materialize step 220 into an interface stream that we can dump for testing

CREATE OR REPLACE PUMP "interface"."fe_pipeline_step_200_pump" STOPPED
AS
INSERT INTO "interface"."fe_pipeline_step_200"
( "transaction_id"
, "tenantId"
, "signals" 
, "headers" 
, "neustar" 
, "eval" 
, "score" 
, "user_id"
, "device_id"
, "fname"
, "fno"
, "fvalue"
)
SELECT STREAM 
  "transaction_id"
, "tenantId"
, "signals" 
, "headers" 
, "neustar" 
, "eval" 
, "score" 
, "user_id"
, "device_id"
, "fname"
, "fno"
, "fvalue"
FROM "fe_pipeline_step_200";


-- 220: extract the confidence from city/state/country features
-- note we are selecting from the materialized stream

CREATE OR REPLACE VIEW "fe_pipeline_step_220"
AS 
SELECT STREAM 
  "transaction_id"
, "tenantId"
, "signals" 
, "headers" 
, "neustar" 
, "eval" 
, "score" 
, "user_id"
, "device_id"
, "fname"
, "fno"
, CASE 
  WHEN "fname" in ('gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_city'
                  ,'gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country'
                  ,'gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state'
                  ) 
  THEN SUBSTRING("fvalue" FROM 1 FOR POSITION(':' IN "fvalue")-1)
  ELSE "fvalue"
  END AS "fvalue"
, CASE 
  WHEN "fname" in ('gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_city'
                  ,'gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country'
                  ,'gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state'
                  ) 
  THEN CAST(SUBSTRING("fvalue" FROM POSITION(':' IN "fvalue")+1) AS INTEGER)
  ELSE CAST(NULL AS INTEGER)
  END AS "confidence"
--FROM "fe_pipeline_step_200"
FROM "interface"."fe_pipeline_step_200"
;

-- step 500 - build the Windows


-- Calculating the Windows
-- ========================


-- Special treatment for confidence
-- ================================
-- (Algorithm: Kuldeep Jiwani)
-- The locations are being provided by a third party vendor, which also provides a confidence score for each location
-- Confidence values are present in data as {CountryData_country_cf, StateData_state_cf, CityData_city_cf}
-- Each of them contains a percent confidence about location in the range 0 – 100%
-- For the isin window function where we are calculating % presence in window we find percentage of occurrence of current value in window counting it as 1 each time we encounter it
-- But for the above 3 location features we first multiply it with their confidence and then compute the isin window %
-- For example: If city in window were: {SFO, SJ, SFO, SFO} and current city is SFO then by normal calculation % presence of current city would be ¾ = 0.75
-- But let’s say the confidence for location SFO is 80% and SJ is 60%, then while counting we will count SFO as 0.8 and SJ as 0.6 instead of 1
-- So now % presence of current city is = (3 * 0.8) / 4 = 0.6
-- This is done for all 3 location features in all windows



CREATE OR REPLACE VIEW "fe_pipeline_step_500"
AS
SELECT STREAM
  "transaction_id"
, "tenantId"
, "signals" 
, "headers" 
, "neustar" 
, "eval" 
, "score" 
, "user_id"
, "device_id"
, "fname" 
, "fno"
, "fvalue"
-- 1 HOUR WINDOWS
, CASE 
  WHEN "confidence" IS NOT NULL    -- one of the location features
  THEN
      SUM("confidence") OVER (PARTITION BY "user_id", "fname", "fvalue" RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND INTERVAL '0.001' SECOND PRECEDING) 
  ELSE                             -- 'normal' feature
      COUNT("fvalue") OVER (PARTITION BY "user_id", "fname","fvalue" RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND INTERVAL '0.001' SECOND PRECEDING) 
  END as "num_usr_win_1h"
, CASE 
  WHEN "confidence" IS NOT NULL    -- one of the location features
  THEN
      100 * COUNT(*) OVER (PARTITION BY "fname" RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND INTERVAL '0.001' SECOND PRECEDING) 
  ELSE                             -- 'normal' feature
      COUNT("fvalue") OVER (PARTITION BY "fname" RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND INTERVAL '0.001' SECOND PRECEDING)
  END as "denom_usr_win_1h"
, CASE 
  WHEN "confidence" IS NOT NULL    -- one of the location features
  THEN
      SUM("confidence") OVER (PARTITION BY "device_id", "fname", "fvalue" RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND INTERVAL '0.001' SECOND PRECEDING) 
  ELSE                             -- 'normal' feature
      COUNT("fvalue") OVER (PARTITION BY "device_id", "fname","fvalue" RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND INTERVAL '0.001' SECOND PRECEDING) 
  END as "num_dev_win_1h"
, CASE 
  WHEN "confidence" IS NOT NULL    -- one of the location features
  THEN
      100 * COUNT(*) OVER (PARTITION BY "fname" RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND INTERVAL '0.001' SECOND PRECEDING) 
  ELSE                             -- 'normal' feature
      COUNT("fvalue") OVER (PARTITION BY "fname" RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND INTERVAL '0.001' SECOND PRECEDING)
  END as "denom_dev_win_1h"
, COUNT(DISTINCT "fvalue") OVER (PARTITION BY "user_id", "fname" RANGE INTERVAL '1' HOUR PRECEDING)
  AS "usr_vel_1h"
, COUNT(DISTINCT "fvalue") OVER (PARTITION BY "device_id", "fname" RANGE INTERVAL '1' HOUR PRECEDING)
  AS "dev_vel_1h"
-- 6 HOUR WINDOWS
, CASE 
  WHEN "confidence" IS NOT NULL    -- one of the location features
  THEN
      SUM("confidence") OVER (PARTITION BY "user_id", "fname", "fvalue" RANGE BETWEEN INTERVAL '6' HOUR PRECEDING AND INTERVAL '0.001' SECOND PRECEDING) 
  ELSE                             -- 'normal' feature
      COUNT("fvalue") OVER (PARTITION BY "user_id", "fname","fvalue" RANGE BETWEEN INTERVAL '6' HOUR PRECEDING AND INTERVAL '0.001' SECOND PRECEDING) 
  END as "num_usr_win_6h"
, CASE 
  WHEN "confidence" IS NOT NULL    -- one of the location features
  THEN
      100 * COUNT(*) OVER (PARTITION BY "fname" RANGE BETWEEN INTERVAL '6' HOUR PRECEDING AND INTERVAL '0.001' SECOND PRECEDING) 
  ELSE                             -- 'normal' feature
      COUNT("fvalue") OVER (PARTITION BY "fname" RANGE BETWEEN INTERVAL '6' HOUR PRECEDING AND INTERVAL '0.001' SECOND PRECEDING)
  END as "denom_usr_win_6h"
, CASE 
  WHEN "confidence" IS NOT NULL    -- one of the location features
  THEN
      SUM("confidence") OVER (PARTITION BY "device_id", "fname", "fvalue" RANGE BETWEEN INTERVAL '6' HOUR PRECEDING AND INTERVAL '0.001' SECOND PRECEDING) 
  ELSE                             -- 'normal' feature
      COUNT("fvalue") OVER (PARTITION BY "device_id", "fname","fvalue" RANGE BETWEEN INTERVAL '6' HOUR PRECEDING AND INTERVAL '0.001' SECOND PRECEDING) 
  END as "num_dev_win_6h"
, CASE 
  WHEN "confidence" IS NOT NULL    -- one of the location features
  THEN
      100 * COUNT(*) OVER (PARTITION BY "fname" RANGE BETWEEN INTERVAL '6' HOUR PRECEDING AND INTERVAL '0.001' SECOND PRECEDING) 
  ELSE                             -- 'normal' feature
      COUNT("fvalue") OVER (PARTITION BY "fname" RANGE BETWEEN INTERVAL '6' HOUR PRECEDING AND INTERVAL '0.001' SECOND PRECEDING)
  END as "denom_dev_win_6h"
, COUNT(DISTINCT "fvalue") OVER (PARTITION BY "user_id", "fname" RANGE INTERVAL '6' HOUR PRECEDING)
  AS "usr_vel_6h"
, COUNT(DISTINCT "fvalue") OVER (PARTITION BY "device_id", "fname" RANGE INTERVAL '6' HOUR PRECEDING)
  AS "dev_vel_6h"

-- 1 DAY WINDOWS - 24h
, CASE 
  WHEN "confidence" IS NOT NULL    -- one of the location features
  THEN
      SUM("confidence") OVER (PARTITION BY "user_id", "fname", "fvalue" RANGE BETWEEN INTERVAL '24' HOUR PRECEDING AND INTERVAL '0.001' SECOND PRECEDING) 
  ELSE                             -- 'normal' feature
      COUNT("fvalue") OVER (PARTITION BY "user_id", "fname","fvalue" RANGE BETWEEN INTERVAL '24' HOUR PRECEDING AND INTERVAL '0.001' SECOND PRECEDING) 
  END as "num_usr_win_24h"
, CASE 
  WHEN "confidence" IS NOT NULL    -- one of the location features
  THEN
      100 * COUNT(*) OVER (PARTITION BY "fname" RANGE BETWEEN INTERVAL '24' HOUR PRECEDING AND INTERVAL '0.001' SECOND PRECEDING) 
  ELSE                             -- 'normal' feature
      COUNT("fvalue") OVER (PARTITION BY "fname" RANGE BETWEEN INTERVAL '24' HOUR PRECEDING AND INTERVAL '0.001' SECOND PRECEDING)
  END as "denom_usr_win_24h"
, CASE 
  WHEN "confidence" IS NOT NULL    -- one of the location features
  THEN
      SUM("confidence") OVER (PARTITION BY "device_id", "fname", "fvalue" RANGE BETWEEN INTERVAL '24' HOUR PRECEDING AND INTERVAL '0.001' SECOND PRECEDING) 
  ELSE                             -- 'normal' feature
      COUNT("fvalue") OVER (PARTITION BY "device_id", "fname","fvalue" RANGE BETWEEN INTERVAL '24' HOUR PRECEDING AND INTERVAL '0.001' SECOND PRECEDING) 
  END as "num_dev_win_24h"
, CASE 
  WHEN "confidence" IS NOT NULL    -- one of the location features
  THEN
      100 * COUNT(*) OVER (PARTITION BY "fname" RANGE BETWEEN INTERVAL '24' HOUR PRECEDING AND INTERVAL '0.001' SECOND PRECEDING) 
  ELSE                             -- 'normal' feature
      COUNT("fvalue") OVER (PARTITION BY "fname" RANGE BETWEEN INTERVAL '24' HOUR PRECEDING AND INTERVAL '0.001' SECOND PRECEDING)
  END as "denom_dev_win_24h"
, COUNT(DISTINCT "fvalue") OVER (PARTITION BY "user_id", "fname" RANGE INTERVAL '24' HOUR PRECEDING)
  AS "usr_vel_24h"
, COUNT(DISTINCT "fvalue") OVER (PARTITION BY "device_id", "fname" RANGE INTERVAL '24' HOUR PRECEDING)
  AS "dev_vel_24h"

-- 1 WEEK WINDOWS - 168h
, CASE 
  WHEN "confidence" IS NOT NULL    -- one of the location features
  THEN
      SUM("confidence") OVER (PARTITION BY "user_id", "fname", "fvalue" RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND INTERVAL '0.001' SECOND PRECEDING) 
  ELSE                             -- 'normal' feature
      COUNT("fvalue") OVER (PARTITION BY "user_id", "fname","fvalue" RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND INTERVAL '0.001' SECOND PRECEDING) 
  END as "num_usr_win_168h"
, CASE 
  WHEN "confidence" IS NOT NULL    -- one of the location features
  THEN
      100 * COUNT(*) OVER (PARTITION BY "fname" RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND INTERVAL '0.001' SECOND PRECEDING) 
  ELSE                             -- 'normal' feature
      COUNT("fvalue") OVER (PARTITION BY "fname" RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND INTERVAL '0.001' SECOND PRECEDING)
  END as "denom_usr_win_168h"
, CASE 
  WHEN "confidence" IS NOT NULL    -- one of the location features
  THEN
      SUM("confidence") OVER (PARTITION BY "device_id", "fname", "fvalue" RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND INTERVAL '0.001' SECOND PRECEDING) 
  ELSE                             -- 'normal' feature
      COUNT("fvalue") OVER (PARTITION BY "device_id", "fname","fvalue" RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND INTERVAL '0.001' SECOND PRECEDING) 
  END as "num_dev_win_168h"
, CASE 
  WHEN "confidence" IS NOT NULL    -- one of the location features
  THEN
      100 * COUNT(*) OVER (PARTITION BY "fname" RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND INTERVAL '0.001' SECOND PRECEDING) 
  ELSE                             -- 'normal' feature
      COUNT("fvalue") OVER (PARTITION BY "fname" RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND INTERVAL '0.001' SECOND PRECEDING)
  END as "denom_dev_win_168h"
, COUNT(DISTINCT "fvalue") OVER (PARTITION BY "user_id", "fname" RANGE INTERVAL '7' DAY PRECEDING)
  AS "usr_vel_168h"
, COUNT(DISTINCT "fvalue") OVER (PARTITION BY "device_id", "fname" RANGE INTERVAL '7' DAY PRECEDING)
  AS "dev_vel_168h"

-- 1 MONTH (30 DAY) WINDOWS - s-Server does not currently support INTERVALs in months for windows
, CASE 
  WHEN "confidence" IS NOT NULL    -- one of the location features
  THEN
      SUM("confidence") OVER (PARTITION BY "user_id", "fname", "fvalue" RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND INTERVAL '0.001' SECOND PRECEDING) 
  ELSE                             -- 'normal' feature
      COUNT("fvalue") OVER (PARTITION BY "user_id", "fname","fvalue" RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND INTERVAL '0.001' SECOND PRECEDING) 
  END as "num_usr_win_720h"
, CASE 
  WHEN "confidence" IS NOT NULL    -- one of the location features
  THEN
      100 * COUNT(*) OVER (PARTITION BY "fname" RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND INTERVAL '0.001' SECOND PRECEDING) 
  ELSE                             -- 'normal' feature
      COUNT("fvalue") OVER (PARTITION BY "fname" RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND INTERVAL '0.001' SECOND PRECEDING)
  END as "denom_usr_win_720h"
, CASE 
  WHEN "confidence" IS NOT NULL    -- one of the location features
  THEN
      SUM("confidence") OVER (PARTITION BY "device_id", "fname", "fvalue" RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND INTERVAL '0.001' SECOND PRECEDING) 
  ELSE                             -- 'normal' feature
      COUNT("fvalue") OVER (PARTITION BY "device_id", "fname","fvalue" RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND INTERVAL '0.001' SECOND PRECEDING) 
  END as "num_dev_win_720h"
, CASE 
  WHEN "confidence" IS NOT NULL    -- one of the location features
  THEN
      100 * COUNT(*) OVER (PARTITION BY "fname" RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND INTERVAL '0.001' SECOND PRECEDING) 
  ELSE                             -- 'normal' feature
      COUNT("fvalue") OVER (PARTITION BY "fname" RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND INTERVAL '0.001' SECOND PRECEDING)
  END as "denom_dev_win_720h"
, COUNT(DISTINCT "fvalue") OVER (PARTITION BY "user_id", "fname" RANGE INTERVAL '30' DAY PRECEDING)
  AS "usr_vel_720h"
, COUNT(DISTINCT "fvalue") OVER (PARTITION BY "device_id", "fname" RANGE INTERVAL '30' DAY PRECEDING)
  AS "dev_vel_720h"

FROM "fe_pipeline_step_220";



CREATE OR REPLACE PUMP "interface"."fe_pipeline_step_500_pump" STOPPED
AS
INSERT INTO "interface"."fe_pipeline_step_500"
( "transaction_id"
, "tenantId"
, "signals" 
, "headers" 
, "neustar" 
, "eval" 
, "score" 
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
, "tenantId"
, "signals" 
, "headers" 
, "neustar" 
, "eval" 
, "score" 
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
FROM "fe_pipeline_step_500";


CREATE OR REPLACE FUNCTION "divide"("numerator" DOUBLE, "denominator" DOUBLE)
RETURNS DOUBLE
LANGUAGE SQL
DETERMINISTIC
CONTAINS SQL
RETURN CASE 
       WHEN "denominator" IS NULL OR "denominator" = 0.0 THEN -0.2
       WHEN "numerator" IS NULL OR "numerator" = 0.0 THEN 0.0
       ELSE "numerator" / "denominator"
       END;

-- this step reads from the materialized stream 

CREATE OR REPLACE VIEW "fe_pipeline_step_510"
AS
SELECT STREAM
  "transaction_id"
, "tenantId"
, "signals" 
, "headers" 
, "neustar" 
, "eval" 
, "score" 
, "user_id"
, "device_id"
, "fname" 
, "fno"
, "fvalue"
-- reorder fields as per Kuldeep's presentation
-- presence
, "divide"("num_dev_win_168h","denom_dev_win_168h") as "dev_win_168h"
, "divide"("num_dev_win_1h","denom_dev_win_1h") as "dev_win_1h"
, "divide"("num_dev_win_24h","denom_dev_win_24h") as "dev_win_24h"
, "divide"("num_dev_win_6h","denom_dev_win_6h") as "dev_win_6h"
, "divide"("num_dev_win_720h","denom_dev_win_720h") as "dev_win_720h" 
, "divide"("num_usr_win_168h","denom_usr_win_168h") as "usr_win_168h"
, "divide"("num_usr_win_1h","denom_usr_win_1h") as "usr_win_1h"
, "divide"("num_usr_win_24h","denom_usr_win_24h") as "usr_win_24h"
, "divide"("num_usr_win_6h","denom_usr_win_6h") as "usr_win_6h"
, "divide"("num_usr_win_720h","denom_usr_win_720h") as "usr_win_720h" 
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
--FROM "fe_pipeline_step_500"
FROM "interface"."fe_pipeline_step_500"
;

-- Scale the derived features using training model
-- ================================================

-- "NORMAL" scaler
-- The 1140 window features are then normalised, which means during training we subtracted each feature by its mean (μ) and divided 
-- by its std. deviation (σ). Now we need to do the same for prediction and we already have a list of these values for each feature 
-- saved in a file like {(μ1, σ1), (μ2, σ2), (μ3, σ3), …( μ1140, σ1140)}.
CREATE OR REPLACE FUNCTION "normal-scaler"("value" DOUBLE, "mean" DOUBLE, "stddev" DOUBLE)
RETURNS VARCHAR(32)
LANGUAGE SQL
DETERMINISTIC
CONTAINS SQL
RETURN CAST(
        CASE 
        WHEN "stddev" IS NULL OR "stddev" = 0.0 THEN coalesce("mean",0.0)
        ELSE ("value" - "mean")/"stddev"
        END
       AS VARCHAR(32));

-- "Min/Max" scaler
-- The rescaled value for a feature E is calculated as: 
-- Rescaled(Ei) = (Ei – Emin)  /  (Emax – Emin).
-- For the case Emax == Emin Rescaled(ei)=0.5
CREATE OR REPLACE FUNCTION "minmax-scaler"("value" DOUBLE, "min" DOUBLE, "max" DOUBLE)
RETURNS VARCHAR(32)
LANGUAGE SQL
DETERMINISTIC
CONTAINS SQL
RETURN CAST(
        CASE 
        WHEN "min" = "max" THEN 0.5
        ELSE ("value" - "min")/("max" - "min")
        END
       AS VARCHAR(32));


-- see fpp_normalize_min_max.txt and normalization_data.sql


CREATE OR REPLACE VIEW "fe_pipeline_step_520"
AS
SELECT STREAM
  f."transaction_id"
, f."tenantId"
, f."signals" 
, f."headers" 
, f."neustar" 
, f."eval" 
, f."score" 
, f."user_id"
, f."device_id"
, f."fname" 
, f."fno"
, f."fvalue"
-- reorder fields as per Kuldeep's presentation
-- presence
, "minmax-scaler"(f."dev_win_168h","min_dev_win_168h","max_dev_win_168h") as "dev_win_168h"
, "minmax-scaler"(f."dev_win_1h",n."min_dev_win_1h",n."max_dev_win_1h") as "dev_win_1h"
, "minmax-scaler"(f."dev_win_24h",n."min_dev_win_24h",n."max_dev_win_24h") as "dev_win_24h"
, "minmax-scaler"(f."dev_win_6h",n."min_dev_win_6h",n."max_dev_win_6h") as "dev_win_6h"
, "minmax-scaler"(f."dev_win_720h",n."min_dev_win_720h",n."max_dev_win_720h") as "dev_win_720h" 
--
, "minmax-scaler"(f."usr_win_168h",n."min_usr_win_168h",n."max_usr_win_168h") as "usr_win_168h"
, "minmax-scaler"(f."usr_win_1h",n."min_usr_win_1h",n."max_usr_win_1h") as "usr_win_1h"
, "minmax-scaler"(f."usr_win_24h",n."min_usr_win_24h",n."max_usr_win_24h") as "usr_win_24h"
, "minmax-scaler"(f."usr_win_6h",n."min_usr_win_6h",n."max_usr_win_6h") as "usr_win_6h"
, "minmax-scaler"(f."usr_win_720h",n."min_usr_win_720h",n."max_usr_win_720h") as "usr_win_720h" 
-- velocity
, "minmax-scaler"(f."dev_vel_168h",n."min_dev_vel_168h",n."max_dev_vel_168h") as "dev_vel_168h"
, "minmax-scaler"(f."dev_vel_1h",n."min_dev_vel_1h",n."max_dev_vel_1h") as "dev_vel_1h"
, "minmax-scaler"(f."dev_vel_24h",n."min_dev_vel_24h",n."max_dev_vel_24h") as "dev_vel_24h"
, "minmax-scaler"(f."dev_vel_6h",n."min_dev_vel_6h",n."max_dev_vel_6h") as "dev_vel_6h"
, "minmax-scaler"(f."dev_vel_720h",n."min_dev_vel_720h",n."max_dev_vel_720h") as "dev_vel_720h" 
--
, "minmax-scaler"(f."usr_vel_168h",n."min_usr_vel_168h",n."max_usr_vel_168h") as "usr_vel_168h"
, "minmax-scaler"(f."usr_vel_1h",n."min_usr_vel_1h",n."max_usr_vel_1h") as "usr_vel_1h"
, "minmax-scaler"(f."usr_vel_24h",n."min_usr_vel_24h",n."max_usr_vel_24h") as "usr_vel_24h"
, "minmax-scaler"(f."usr_vel_6h",n."min_usr_vel_6h",n."max_usr_vel_6h") as "usr_vel_6h"
, "minmax-scaler"(f."usr_vel_720h",n."min_usr_vel_720h",n."max_usr_vel_720h") as "usr_vel_720h" 
FROM "fe_pipeline_step_510" f
JOIN "fe_normalization_flattened" n ON n."raw_fname" = f."fname";



-- Now we need to pivot back to a single row, and present the data as an array in the expected order.




CREATE OR REPLACE VIEW "fe_pipeline_step_600"
AS
SELECT STREAM
  "transaction_id"
, "tenantId"
, "signals" 
, "headers" 
, "neustar" 
, "eval" 
, "score" 
, "user_id"
, "device_id"
, "fname" 
, "fno"
, coalesce("dev_win_168h",'null') || ',' ||
  coalesce("dev_win_1h" ,'null')  || ',' ||
  coalesce("dev_win_24h" ,'null')  || ',' ||
  coalesce("dev_win_6h" ,'null')  || ',' ||
  coalesce("dev_win_720h" ,'null')  || ',' ||
  coalesce("usr_win_168h" ,'null')  || ',' ||
  coalesce("usr_win_1h" ,'null')  || ',' ||
  coalesce("usr_win_24h" ,'null')  || ',' ||
  coalesce("usr_win_6h" ,'null')  || ',' ||
  coalesce("usr_win_720h" ,'null')  || '|' ||  -- pipe separator here allows us split win from vel later
-- velocity
  coalesce("dev_vel_168h" ,'null')  || ',' ||
  coalesce("dev_vel_1h" ,'null')  || ',' ||
  coalesce("dev_vel_24h" ,'null')  || ',' ||
  coalesce("dev_vel_6h" ,'null')  || ',' ||
  coalesce("dev_vel_720h" ,'null')  || ',' ||
  coalesce("usr_vel_168h" ,'null')  || ',' ||
  coalesce("usr_vel_1h" ,'null')  || ',' ||
  coalesce("usr_vel_24h" ,'null')  || ',' ||
  coalesce("usr_vel_6h" ,'null')  || ',' ||
  coalesce("usr_vel_720h" ,'null')  
     AS "derived_value"
FROM "fe_pipeline_step_520";


-- Now do the pivot using PivotUdx

CREATE OR REPLACE FUNCTION "pivot_features"
( "inputRows" CURSOR 
, "uidColumnNames" VARCHAR(4096)
, "keyColumnName" VARCHAR(128)
, "valueColumnName" VARCHAR(128)
) RETURNS TABLE
( ROWTIME TIMESTAMP
, "transaction_id" varchar(64)
, "tenantId" VARCHAR(256)
, "signals" VARCHAR(102400)
, "headers" VARCHAR(102400)
, "neustar" VARCHAR(102400)
, "eval" VARCHAR(102400)
, "score" VARCHAR(1024)
, "device_id" varchar(64)
, "user_id" varchar(64)
-- the following columns are all the original raw features
, "context_client" varchar(500)
, "context_events" varchar(500)
, "context_flag001" varchar(500)
, "context_flag002" varchar(500)
, "context_usergroups" varchar(500)
, "day_of_week" varchar(500)
, "gemaltoRiskEngine_attributes_deviceBrowser_browserName" varchar(500)
, "gemaltoRiskEngine_attributes_deviceBrowser_browserVersion" varchar(500)
, "gemaltoRiskEngine_attributes_deviceBrowser_isRooted" varchar(500)
, "gemaltoRiskEngine_attributes_deviceBrowser_networkIp" varchar(500)
, "gemaltoRiskEngine_attributes_deviceBrowser_osFamily" varchar(500)
, "gemaltoRiskEngine_attributes_deviceBrowser_osName" varchar(500)
, "gemaltoRiskEngine_attributes_deviceBrowser_osReleaseDateOrder" varchar(500)
, "gemaltoRiskEngine_attributes_deviceBrowser_osVersionOfFamily" varchar(500)
, "gemaltoRiskEngine_attributes_deviceBrowser_osVersion" varchar(500)
, "gemaltoRiskEngine_attributes_deviceBrowser_screenHeight" varchar(500)
, "gemaltoRiskEngine_attributes_deviceBrowser_screenWidth" varchar(500)
, "gemaltoRiskEngine_attributes_deviceBrowser_userAgent" varchar(500)
, "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_fingerprint" varchar(500)
, "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_manufacturer" varchar(500)
, "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_model" varchar(500)
, "gemaltoRiskEngine_attributes_deviceMobileApp_location_signals_source" varchar(500)
, "gemaltoRiskEngine_attributes_deviceMobileApp_network_signals_hwAddress" varchar(500)
, "gemaltoRiskEngine_attributes_deviceMobileApp_network_signals_type" varchar(500)
, "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isBluetoothEnabled" varchar(500)
, "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isLocationEnabled" varchar(500)
, "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isWifiEnabled" varchar(500)
, "gemaltoRiskEngine_attributes_deviceMobileApp_platform_signals_family" varchar(500)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_area_code" varchar(500)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_city" varchar(500)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_postal_code" varchar(500)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_time_zone" varchar(500)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country_code" varchar(500)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country" varchar(500)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state_code" varchar(500)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state" varchar(500)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_continent" varchar(500)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_dma" varchar(500)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_latitude" varchar(500)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_longitude" varchar(500)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_msa" varchar(500)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_region" varchar(500)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_Domain_sld" varchar(500)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_Domain_tld" varchar(500)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_asn" varchar(500)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_carrier" varchar(500)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_connection_type" varchar(500)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_hosting_facility" varchar(500)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_ip_routing_type" varchar(500)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_line_speed" varchar(500)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_organization" varchar(500)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ProxyData_proxy_level" varchar(500)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ProxyData_proxy_type" varchar(500)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_anonymizer_status" varchar(500)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ip_type" varchar(500)
, "hour_of_day" varchar(500)
, "visitId" varchar(500)
)
LANGUAGE JAVA
PARAMETER STYLE SYSTEM DEFINED JAVA
NO SQL
EXTERNAL NAME 'PIVOT:com.sqlstream.udx.PivotUdx.execute';

-- pivot back to a single row per transaction

CREATE OR REPLACE VIEW "fe_pipeline_step_620"
AS
SELECT STREAM * 
FROM STREAM
  ( "pivot_features"
    ( CURSOR (SELECT STREAM * FROM "fe_pipeline_step_600")
    , 'transaction_id'
    , 'fname'
    , 'derived_value'
    )
  );


-- Next we need to take the values from each of the feature value columns and put them together into a large array
-- in the same order as shown in fpp_features_list.txt
-- Unfortunately the array ordering is not convenient. We can't just concatenate the data we already have, we have to interlace it.
-- We have divided each list of values with a pipe so we have <win features>|<vel features> in each of the columnList
-- In the regex we have to escape the pipe '\|.*' to cut the second half and '.*\|' to cut the first half

CREATE OR REPLACE VIEW "fe_pipeline_step_650"
AS
SELECT STREAM 
  "transaction_id"
, "tenantId"
, "signals" 
, "headers" 
, "neustar" 
, "eval" 
, "score" 
, "user_id"
, "device_id"
-- get first half of each combined field for the xxx_win values
, REGEX_REPLACE("context_client",'\|.*','',1,1) as "context_client_win"
, REGEX_REPLACE("context_events",'\|.*','',1,1) AS "context_events_win" 
, REGEX_REPLACE("context_flag001",'\|.*','',1,1) AS "context_flag001_win" 
, REGEX_REPLACE("context_flag002",'\|.*','',1,1) AS "context_flag002_win" 
, REGEX_REPLACE("context_usergroups",'\|.*','',1,1) AS "context_usergroups_win" 
, REGEX_REPLACE("day_of_week",'\|.*','',1,1) AS "day_of_week_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceBrowser_browserName",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_deviceBrowser_browserName_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceBrowser_browserVersion",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_deviceBrowser_browserVersion_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceBrowser_isRooted",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_deviceBrowser_isRooted_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceBrowser_networkIp",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_deviceBrowser_networkIp_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceBrowser_osFamily",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_deviceBrowser_osFamily_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceBrowser_osName",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_deviceBrowser_osName_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceBrowser_osReleaseDateOrder",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_deviceBrowser_osReleaseDateOrder_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceBrowser_osVersionOfFamily",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_deviceBrowser_osVersionOfFamily_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceBrowser_osVersion",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_deviceBrowser_osVersion_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceBrowser_screenHeight",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_deviceBrowser_screenHeight_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceBrowser_screenWidth",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_deviceBrowser_screenWidth_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceBrowser_userAgent",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_deviceBrowser_userAgent_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_fingerprint",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_fingerprint_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_manufacturer",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_manufacturer_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_model",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_model_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceMobileApp_location_signals_source",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_deviceMobileApp_location_signals_source_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceMobileApp_network_signals_hwAddress",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_deviceMobileApp_network_signals_hwAddress_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceMobileApp_network_signals_type",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_deviceMobileApp_network_signals_type_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isBluetoothEnabled",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isBluetoothEnabled_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isLocationEnabled",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isLocationEnabled_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isWifiEnabled",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isWifiEnabled_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceMobileApp_platform_signals_family",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_deviceMobileApp_platform_signals_family_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_area_code",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_area_code_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_city",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_city_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_postal_code",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_postal_code_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_time_zone",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_time_zone_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country_code",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country_code_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state_code",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state_code_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state_win"  
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_continent",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_continent_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_dma",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_dma_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_latitude",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_latitude_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_longitude",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_longitude_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_msa",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_msa_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_region",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_region_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_Domain_sld",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_Domain_sld_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_Domain_tld",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_Domain_tld_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_asn",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_asn_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_carrier",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_carrier_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_connection_type",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_connection_type_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_hosting_facility",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_hosting_facility_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_ip_routing_type",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_ip_routing_type_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_line_speed",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_line_speed_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_organization",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_organization_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ProxyData_proxy_level",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ProxyData_proxy_level_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ProxyData_proxy_type",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ProxyData_proxy_type_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_anonymizer_status",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_anonymizer_status_win" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ip_type",'\|.*','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ip_type_win" 
, REGEX_REPLACE("hour_of_day",'\|.*','',1,1) AS "hour_of_day_win" 
, REGEX_REPLACE("visitId",'\|.*','',1,1) AS "visitId_win" 
-- get second half of each combined field for the xxx_vel values
, REGEX_REPLACE("context_client",'.*\|','',1,1) as "context_client_vel"
, REGEX_REPLACE("context_events",'.*\|','',1,1) AS "context_events_vel" 
, REGEX_REPLACE("context_flag001",'.*\|','',1,1) AS "context_flag001_vel" 
, REGEX_REPLACE("context_flag002",'.*\|','',1,1) AS "context_flag002_vel" 
, REGEX_REPLACE("context_usergroups",'.*\|','',1,1) AS "context_usergroups_vel" 
, REGEX_REPLACE("day_of_week",'.*\|','',1,1) AS "day_of_week_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceBrowser_browserName",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_deviceBrowser_browserName_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceBrowser_browserVersion",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_deviceBrowser_browserVersion_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceBrowser_isRooted",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_deviceBrowser_isRooted_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceBrowser_networkIp",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_deviceBrowser_networkIp_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceBrowser_osFamily",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_deviceBrowser_osFamily_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceBrowser_osName",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_deviceBrowser_osName_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceBrowser_osReleaseDateOrder",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_deviceBrowser_osReleaseDateOrder_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceBrowser_osVersionOfFamily",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_deviceBrowser_osVersionOfFamily_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceBrowser_osVersion",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_deviceBrowser_osVersion_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceBrowser_screenHeight",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_deviceBrowser_screenHeight_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceBrowser_screenWidth",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_deviceBrowser_screenWidth_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceBrowser_userAgent",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_deviceBrowser_userAgent_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_fingerprint",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_fingerprint_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_manufacturer",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_manufacturer_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_model",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_model_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceMobileApp_location_signals_source",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_deviceMobileApp_location_signals_source_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceMobileApp_network_signals_hwAddress",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_deviceMobileApp_network_signals_hwAddress_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceMobileApp_network_signals_type",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_deviceMobileApp_network_signals_type_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isBluetoothEnabled",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isBluetoothEnabled_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isLocationEnabled",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isLocationEnabled_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isWifiEnabled",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isWifiEnabled_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_deviceMobileApp_platform_signals_family",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_deviceMobileApp_platform_signals_family_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_area_code",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_area_code_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_city",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_city_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_postal_code",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_postal_code_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_time_zone",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_time_zone_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country_code",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country_code_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state_code",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state_code_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state_vel"  
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_continent",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_continent_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_dma",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_dma_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_latitude",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_latitude_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_longitude",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_longitude_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_msa",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_msa_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_region",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_region_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_Domain_sld",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_Domain_sld_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_Domain_tld",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_Domain_tld_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_asn",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_asn_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_carrier",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_carrier_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_connection_type",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_connection_type_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_hosting_facility",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_hosting_facility_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_ip_routing_type",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_ip_routing_type_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_line_speed",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_line_speed_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_organization",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_organization_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ProxyData_proxy_level",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ProxyData_proxy_level_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ProxyData_proxy_type",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ProxyData_proxy_type_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_anonymizer_status",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_anonymizer_status_vel" 
, REGEX_REPLACE("gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ip_type",'.*\|','',1,1) AS "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ip_type_vel" 
, REGEX_REPLACE("hour_of_day",'.*\|','',1,1) AS "hour_of_day_vel" 
, REGEX_REPLACE("visitId",'.*\|','',1,1) AS "visitId_vel" 
FROM "feature_engineering"."fe_pipeline_step_620";

-- Now we can combine these into a single character strings


CREATE OR REPLACE VIEW "fe_pipeline_step_660"
AS
SELECT STREAM
  "transaction_id" 
, "tenantId"
, "signals" 
, "headers" 
, "neustar" 
, "eval" 
, "score" , "user_id" 
, "device_id" 
, '[[' ||"context_client_win" 
|| ',' ||  "context_events_win" 
|| ',' ||  "context_flag001_win" 
|| ',' ||  "context_flag002_win" 
|| ',' ||  "context_usergroups_win" 
|| ',' ||  "day_of_week_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceBrowser_browserName_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceBrowser_browserVersion_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceBrowser_isRooted_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceBrowser_networkIp_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceBrowser_osFamily_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceBrowser_osName_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceBrowser_osReleaseDateOrder_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceBrowser_osVersionOfFamily_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceBrowser_osVersion_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceBrowser_screenHeight_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceBrowser_screenWidth_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceBrowser_userAgent_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_fingerprint_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_manufacturer_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_model_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceMobileApp_location_signals_source_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceMobileApp_network_signals_hwAddress_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceMobileApp_network_signals_type_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isBluetoothEnabled_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isLocationEnabled_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isWifiEnabled_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceMobileApp_platform_signals_family_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_area_code_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_city_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_postal_code_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_time_zone_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country_code_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state_code_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_continent_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_dma_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_latitude_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_longitude_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_msa_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_region_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_Domain_sld_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_Domain_tld_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_asn_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_carrier_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_connection_type_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_hosting_facility_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_ip_routing_type_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_line_speed_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_organization_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ProxyData_proxy_level_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ProxyData_proxy_type_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_anonymizer_status_win" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ip_type_win" 
|| ',' ||  "hour_of_day_win" 
|| ',' ||  "visitId_win" 
-- and the _vel_ elements
|| ',' ||  "context_client_vel" 
|| ',' ||  "context_events_vel" 
|| ',' ||  "context_flag001_vel" 
|| ',' ||  "context_flag002_vel" 
|| ',' ||  "context_usergroups_vel" 
|| ',' ||  "day_of_week_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceBrowser_browserName_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceBrowser_browserVersion_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceBrowser_isRooted_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceBrowser_networkIp_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceBrowser_osFamily_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceBrowser_osName_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceBrowser_osReleaseDateOrder_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceBrowser_osVersionOfFamily_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceBrowser_osVersion_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceBrowser_screenHeight_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceBrowser_screenWidth_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceBrowser_userAgent_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_fingerprint_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_manufacturer_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_model_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceMobileApp_location_signals_source_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceMobileApp_network_signals_hwAddress_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceMobileApp_network_signals_type_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isBluetoothEnabled_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isLocationEnabled_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isWifiEnabled_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_deviceMobileApp_platform_signals_family_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_area_code_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_city_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_postal_code_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_time_zone_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country_code_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state_code_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_continent_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_dma_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_latitude_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_longitude_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_msa_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_region_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_Domain_sld_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_Domain_tld_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_asn_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_carrier_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_connection_type_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_hosting_facility_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_ip_routing_type_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_line_speed_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_organization_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ProxyData_proxy_level_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ProxyData_proxy_type_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_anonymizer_status_vel" 
|| ',' ||  "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ip_type_vel" 
|| ',' ||  "hour_of_day_vel" 
|| ',' ||  "visitId_vel" 
|| ']]' AS "features_1140"
FROM "fe_pipeline_step_650";

-- deliver data into the output stream for this pipeline

CREATE OR REPLACE PUMP "interface"."fe_pipeline_pump" STOPPED
AS
INSERT INTO "interface"."fe_pipeline_out"
( "transaction_id" 
, "tenantId"
, "signals" 
, "headers" 
, "neustar" 
, "eval" 
, "score" , "user_id" 
, "device_id" 
, "features_1140" 
)
SELECT STREAM 
  "transaction_id" 
, "tenantId"
, "signals" 
, "headers" 
, "neustar" 
, "eval" 
, "score" 
, "user_id" 
, "device_id" 
, "features_1140" 
FROM "feature_engineering"."fe_pipeline_step_660"
;


