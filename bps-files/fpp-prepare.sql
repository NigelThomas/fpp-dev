-- features.sql
-- the feature engineering pipeline drives from interface.transactions
--
--
!set force on
alter pump "interface".* stop;
alter stream "interface"."transactions" reset;
!set force off

CREATE OR REPLACE SCHEMA "fpp-test-score-0.1_v2";
SET SCHEMA '"fpp-test-score-0.1_v2"';
SET PATH '"fpp-test-score-0.1_v2"';

-- ---------------------------------------------------------------------------------------------------------------------
-- EXTERNAL STREAM SOURCE: signals
-- ---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FOREIGN STREAM "signals_raw_fs"
(
    "tenantId" VARCHAR(256),
    "visitId" VARCHAR(64),
    "signals" VARCHAR(1024000),
    "headers" VARCHAR(1024000),
    "neustar" VARCHAR(1024000)
)
   SERVER "KAFKA10_SERVER"
   OPTIONS (
       "PARSER" 'JSON',
       "ROW_PATH" '$',
       "tenantId_PATH" '$.tenantId',
       "visitId_PATH" '$.visitId',
       "signals_PATH" '$.signals',
       "headers_PATH" '$.headers',
       "neustar_PATH" '$.neustar',
       "kafka.consumer.config" '/home/sqlstream/config/kafka.properties',
       "SEED_BROKERS" 'localhost:9092',
       "STARTING_TIME" 'LATEST',
       "MAX_POLL_RECORDS" '100',
       "STARTING_OFFSET" '-1',
       "BUFFER_SIZE" '1048576',
       "FETCH_SIZE" '1000000',
       "isolation.level" 'read_uncommitted',
       "TOPIC" 'signals'
   );

CREATE OR REPLACE STREAM "signals"
(
    "tenantId" VARCHAR(256),
    "visitId" VARCHAR(64),
    "signals" VARCHAR(1024000),
    "headers" VARCHAR(1024000),
    "neustar" VARCHAR(1024000)
);

CREATE OR REPLACE PUMP "source-to-signals-Pump" STOPPED AS
INSERT INTO "signals"
SELECT STREAM * FROM "signals_raw_fs";




-- ---------------------------------------------------------------------------------------------------------------------
-- EXTERNAL STREAM SOURCE: contexts
-- ---------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE FOREIGN STREAM "contexts_raw_fs"
(
    "visitId" VARCHAR(64),
    "eval" VARCHAR(102400)
)
   SERVER "KAFKA10_SERVER"
   OPTIONS (
       "PARSER" 'JSON',
       "ROW_PATH" '$',
       "visitId_PATH" '$.visitId',
       "eval_PATH" '$.eval',
       "kafka.consumer.config" '/home/sqlstream/config/kafka.properties',
       "SEED_BROKERS" 'localhost:9092',
       "STARTING_TIME" 'LATEST',
       "MAX_POLL_RECORDS" '100',
       "STARTING_OFFSET" '-1',
       "BUFFER_SIZE" '1048576',
       "FETCH_SIZE" '1000000',
       "isolation.level" 'read_uncommitted',
       "TOPIC" 'contexts'
   );

CREATE OR REPLACE STREAM "contexts"
(
    "visitId" VARCHAR(64),
    "eval" VARCHAR(102400)
);

CREATE OR REPLACE PUMP "source-to-contexts-Pump" STOPPED AS
INSERT INTO "contexts"
SELECT STREAM * FROM "contexts_raw_fs";


-- ---------------------------------------------------------------------------------------------------------------------
-- PIPELINE: scoring
-- ---------------------------------------------------------------------------------------------------------------------

--  Join contexts and signals / visitId
CREATE OR REPLACE VIEW "scoring_step_1" AS
    SELECT STREAM "signals".*, "contexts"."eval"
    FROM "contexts" AS "contexts"
    INNER JOIN "signals" OVER (RANGE INTERVAL '1' HOUR PRECEDING) AS "signals"
        ON ("contexts"."visitId" = "signals"."visitId")  ;


--  Add parameters used for features computation
CREATE OR REPLACE JAR fpp_udx LIBRARY 'file:/home/sqlstream/fpp/fpp-udx.jar' OPTIONS (0);
CREATE OR REPLACE FUNCTION "extractProperties"(c CURSOR) RETURNS TABLE(
    c.*,
    "context_client" VARCHAR(64),
    "context_events" VARCHAR(64),
    "context_flag001" VARCHAR(64),
    "context_flag002" VARCHAR(64),
    "context_usergroups" VARCHAR(64),
    "gemaltoRiskEngine_attributes_deviceBrowser_browserName" VARCHAR(64),
    "gemaltoRiskEngine_attributes_deviceBrowser_browserVersion" VARCHAR(64),
    "gemaltoRiskEngine_attributes_deviceBrowser_isRooted" VARCHAR(64),
    "gemaltoRiskEngine_attributes_deviceBrowser_networkIp" VARCHAR(64),
    "gemaltoRiskEngine_attributes_deviceBrowser_osFamily" VARCHAR(64),
    "gemaltoRiskEngine_attributes_deviceBrowser_osName" VARCHAR(64),
    "gemaltoRiskEngine_attributes_deviceBrowser_osReleaseDateOrder" VARCHAR(64),
    "gemaltoRiskEngine_attributes_deviceBrowser_osVersionOfFamily" VARCHAR(64),
    "gemaltoRiskEngine_attributes_deviceBrowser_osVersion" VARCHAR(64),
    "gemaltoRiskEngine_attributes_deviceBrowser_screenHeight" VARCHAR(64),
    "gemaltoRiskEngine_attributes_deviceBrowser_screenWidth" VARCHAR(64),
    "gemaltoRiskEngine_attributes_deviceBrowser_userAgent" VARCHAR(128),
    "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_fingerprint" VARCHAR(64),
    "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_manufacturer" VARCHAR(64),
    "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_model" VARCHAR(64),
    "gemaltoRiskEngine_attributes_deviceMobileApp_location_signals_source" VARCHAR(64),
    "gemaltoRiskEngine_attributes_deviceMobileApp_network_signals_hwAddress" VARCHAR(64),
    "gemaltoRiskEngine_attributes_deviceMobileApp_network_signals_type" VARCHAR(64),
    "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isBluetoothEnabled" VARCHAR(64),
    "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isLocationEnabled" VARCHAR(64),
    "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isWifiEnabled" VARCHAR(64),
    "gemaltoRiskEngine_attributes_deviceMobileApp_platform_signals_family" VARCHAR(64),
    "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_area_code" VARCHAR(64),
    "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_city" VARCHAR(64),
    "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_city_cf" VARCHAR(64),
    "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_postal_code" VARCHAR(64),
    "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_time_zone" VARCHAR(64),
    "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country_code" VARCHAR(64),
    "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country" VARCHAR(64),
    "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country_cf" VARCHAR(64),
    "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state_code" VARCHAR(64),
    "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state" VARCHAR(64),
    "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state_cf" VARCHAR(64),
    "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_continent" VARCHAR(64),
    "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_dma" VARCHAR(64),
    "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_latitude" VARCHAR(64),
    "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_longitude" VARCHAR(64),
    "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_msa" VARCHAR(64),
    "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_region" VARCHAR(64),
    "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_Domain_sld" VARCHAR(64),
    "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_Domain_tld" VARCHAR(64),
    "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_asn" VARCHAR(64),
    "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_carrier" VARCHAR(64),
    "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_connection_type" VARCHAR(64),
    "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_hosting_facility" VARCHAR(64),
    "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_ip_routing_type" VARCHAR(64),
    "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_line_speed" VARCHAR(64),
    "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_organization" VARCHAR(64),
    "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ProxyData_proxy_level" VARCHAR(64),
    "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ProxyData_proxy_type" VARCHAR(64),
    "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_anonymizer_status" VARCHAR(64),
    "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ip_type" VARCHAR(64),
    "deviceId" VARCHAR(64),
    "day_of_week" VARCHAR(64),
    "hour_of_day" VARCHAR(64) ) LANGUAGE JAVA PARAMETER STYLE SYSTEM DEFINED JAVA NO SQL EXTERNAL NAME 'fpp_udx:com.sqlstream.udx.UdxCallAPI.addFeatureParams';

CREATE OR REPLACE VIEW "scoring_step_2" AS
    SELECT STREAM *
    FROM STREAM("extractProperties"(CURSOR( SELECT STREAM * FROM "scoring_step_1" AS "input")));


CREATE OR REPLACE PUMP "interface"."transactions_pump" STOPPED
AS
INSERT INTO "interface"."transactions"
( "transaction_id"
, "device_id"
, "user_id"
, "city_cf"
, "state_cf"
, "country_cf"
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
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_city"
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_postal_code"
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_time_zone"
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country_code"
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country"
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state_code"
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state"
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
)
SELECT STREAM
  "visitId"
, "deviceId"
, "context_client"
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_city_cf"
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state_cf"
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country_cf"
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
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_city"
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_postal_code"
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_time_zone"
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country_code"
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country"
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state_code"
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state"
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
FROM "scoring_step_2";
