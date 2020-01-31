-- This reads from sample JSON data 
-- missing features are:
--  f16 - gemaltoRiskEngine_attributes_deviceBrowser_screenHeight
--  f17 - gemaltoRiskEngine_attributes_deviceBrowser_screenWidth
--  f29 - gemaltoRiskEngine_attributes_ipintelf29ligence_ipinfo_Location_CityData_area_code
--  f52 - gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ProxyData_proxy_level
--  f53 - gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ProxyData_proxy_type
--  f54 - gemaltoRiskEngine_attributes_ipintelligence_ipinfo_anonymizer_status
-- 

CREATE OR REPLACE SCHEMA "sample_json_ingest";
ALTER PUMP "sample_json_ingest".* STOP;
DROP SCHEMA "sample_json_ingest" CASCADE;

CREATE OR REPLACE SCHEMA "sample_json_ingest";
SET SCHEMA '"sample_json_ingest"';
SET PATH '"sample_json_ingest"';

CREATE OR REPLACE FOREIGN STREAM "sample_json_ingest"."sample_json_in_fs"
(
    "$oid" VARCHAR(32),
    "tenantId" VARCHAR(4),
    "userId" VARCHAR(32),
    "time" BIGINT,
    "matchedPolicyName" VARCHAR(16),
    "matchedScenarioName" VARCHAR(16),
    "f57_visitId" VARCHAR(64),
    "rulesetExternalId" VARCHAR(8),
    "rulename" BOOLEAN,
    "ruleExpectedValue" BOOLEAN,
    "f10_gemaltoRiskEngine_attributes_deviceBrowser_networkIp" VARCHAR(64),
    "f18_gemaltoRiskEngine_attributes_deviceBrowser_userAgent" VARCHAR(256),
    "f07_gemaltoRiskEngine_attributes_deviceBrowser_browserName" VARCHAR(16),
    "f08_gemaltoRiskEngine_attributes_deviceBrowser_browserVersion" VARCHAR(16),
    "f12_gemaltoRiskEngine_attributes_deviceBrowser_osName" VARCHAR(8),
    "f15_gemaltoRiskEngine_attributes_deviceBrowser_osVersion" VARCHAR(8),
    "f11_gemaltoRiskEngine_attributes_deviceBrowser_osFamily" VARCHAR(8),
    "f14_gemaltoRiskEngine_attributes_deviceBrowser_osVersionOfFamily" DECIMAL(2, 1),
    "f13_gemaltoRiskEngine_attributes_deviceBrowser_osReleaseDateOrder" INTEGER,
    "f09_gemaltoRiskEngine_attributes_deviceBrowser_isRooted" BOOLEAN,
    "ip_address" VARCHAR(64),
    "f55_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ip_type" VARCHAR(8),
    "f51_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_organization" VARCHAR(64),
    "f46_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_carrier" VARCHAR(64),
    "f45_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_asn" INTEGER,
    "f47_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_connection_type" VARCHAR(16),
    "f50_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_line_speed" VARCHAR(8),
    "f49_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_ip_routing_type" VARCHAR(16),
    "f48_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_hosting_facility" BOOLEAN,
    "f44_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_Domain_tld" VARCHAR(8),
    "f43_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_Domain_sld" VARCHAR(16),
    "f37_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_continent" VARCHAR(16),
    "f39_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_latitude" REAL,
    "f40_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_longitude" DOUBLE,
    "f34_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country" VARCHAR(16),
    "f33_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country_code" VARCHAR(4),
    "country_cf" INTEGER,
    "f42_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_region" VARCHAR(16),
    "f36_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state" VARCHAR(32),
    "f35_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state_code" VARCHAR(4),
    "state_cf" INTEGER,
    "f38_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_dma" INTEGER,
    "f41_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_msa" BIGINT,
    "f30_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_city" VARCHAR(32),
    "f31_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_postal_code" VARCHAR(16),
    "f32_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_time_zone" DECIMAL(3,1),
    "city_cf" INTEGER,
    "name" VARCHAR(64),
    "f27_gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isWifiEnabled" BOOLEAN,
    "f26_gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isLocationEnabled" BOOLEAN,
    "f28_gemaltoRiskEngine_attributes_deviceMobileApp_platform_signals_family" VARCHAR(8),
    "version" VARCHAR(8),
    "isRooted0" BOOLEAN,
    "f24_gemaltoRiskEngine_attributes_deviceMobileApp_network_signals_type" VARCHAR(8),
    "f23_gemaltoRiskEngine_attributes_deviceMobileApp_network_signals_hwAddress" VARCHAR(64),
    "f19_gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_fingerprint" VARCHAR(64),
    "f20_gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_manufacturer" VARCHAR(16),
    "f21_gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_model" VARCHAR(16),
    "userId0" VARCHAR(64),
    "f02_context_events" VARCHAR(32),
    "f05_context_usergroups" VARCHAR(16),
    "customerGroupID" VARCHAR(64),
    "f01_context_client" VARCHAR(8),
    "additionalNotes" VARCHAR(32),
    "access" VARCHAR(8),
    "f25_gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isBluetoothEnabled" BOOLEAN,
    "time0" VARCHAR(32),
    "f22_gemaltoRiskEngine_attributes_deviceMobileApp_location_signals_source" VARCHAR(8),
    "latitude0" DOUBLE,
    "longitude0" DOUBLE,
    "accuracy" INTEGER,
    "altitude" DOUBLE,
    "f04_context_flag002" BOOLEAN,
    "f03_context_flag001" BOOLEAN
)

    SERVER "FILE_SERVER"

OPTIONS (
"PARSER" 'JSON',
        "ROW_PATH" '$',
        "$oid_PATH" '$._id.$oid',
        "tenantId_PATH" '$.tenantId',
        "userId_PATH" '$.userId',
        "time_PATH" '$.time',
        "matchedPolicyName_PATH" '$.matchedPolicyName',
        "matchedScenarioName_PATH" '$.matchedScenarioName',
        "f57_visitId_PATH" '$.visitId',
        "rulesetExternalId_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].rulesetExternalId',
        "rulename_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].rulename',
        "ruleExpectedValue_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].ruleExpectedValue',
        "f10_gemaltoRiskEngine_attributes_deviceBrowser_networkIp_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.deviceBrowser.networkIp',
        "f18_gemaltoRiskEngine_attributes_deviceBrowser_userAgent_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.deviceBrowser.userAgent',
        "f07_gemaltoRiskEngine_attributes_deviceBrowser_browserName_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.deviceBrowser.browserName',
        "f08_gemaltoRiskEngine_attributes_deviceBrowser_browserVersion_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.deviceBrowser.browserVersion',
        "f12_gemaltoRiskEngine_attributes_deviceBrowser_osName_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.deviceBrowser.osName',
        "f15_gemaltoRiskEngine_attributes_deviceBrowser_osVersion_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.deviceBrowser.osVersion',
        "f11_gemaltoRiskEngine_attributes_deviceBrowser_osFamily_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.deviceBrowser.osFamily',
        "f14_gemaltoRiskEngine_attributes_deviceBrowser_osVersionOfFamily_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.deviceBrowser.osVersionOfFamily',
        "f13_gemaltoRiskEngine_attributes_deviceBrowser_osReleaseDateOrder_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.deviceBrowser.osReleaseDateOrder',
        "f09_gemaltoRiskEngine_attributes_deviceBrowser_isRooted_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.deviceBrowser.isRooted',
        "ip_address_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.ipintelligence.ipinfo.ip_address',
        "f55_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ip_type_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.ipintelligence.ipinfo.ip_type',
        "f51_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_organization_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.ipintelligence.ipinfo.Network.organization',
        "f46_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_carrier_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.ipintelligence.ipinfo.Network.carrier',
        "f45_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_asn_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.ipintelligence.ipinfo.Network.asn',
        "f47_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_connection_type_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.ipintelligence.ipinfo.Network.connection_type',
        "f50_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_line_speed_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.ipintelligence.ipinfo.Network.line_speed',
        "f49_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_ip_routing_type_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.ipintelligence.ipinfo.Network.ip_routing_type',
        "f48_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_hosting_facility_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.ipintelligence.ipinfo.Network.hosting_facility',
        "f44_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_Domain_tld_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.ipintelligence.ipinfo.Network.Domain.tld',
        "f43_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_Domain_sld_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.ipintelligence.ipinfo.Network.Domain.sld',
        "f37_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_continent_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.ipintelligence.ipinfo.Location.continent',
        "f39_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_latitude_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.ipintelligence.ipinfo.Location.latitude',
        "f40_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_longitude_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.ipintelligence.ipinfo.Location.longitude',
        "f34_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.ipintelligence.ipinfo.Location.CountryData.country',
        "f33_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country_code_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.ipintelligence.ipinfo.Location.CountryData.country_code',
        "country_cf_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.ipintelligence.ipinfo.Location.CountryData.country_cf',
        "f42_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_region_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.ipintelligence.ipinfo.Location.region',
        "f36_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.ipintelligence.ipinfo.Location.StateData.state',
        "f35_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state_code_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.ipintelligence.ipinfo.Location.StateData.state_code',
        "state_cf_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.ipintelligence.ipinfo.Location.StateData.state_cf',
        "f38_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_dma_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.ipintelligence.ipinfo.Location.dma',
        "f41_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_msa_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.ipintelligence.ipinfo.Location.msa',
        "f30_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_city_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.ipintelligence.ipinfo.Location.CityData.city',
        "f31_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_postal_code_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.ipintelligence.ipinfo.Location.CityData.postal_code',
        "f32_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_time_zone_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.ipintelligence.ipinfo.Location.CityData.time_zone',
        "city_cf_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.ipintelligence.ipinfo.Location.CityData.city_cf',
        "name_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.deviceMobileApp.networkSettings.signals.name',
        "f27_gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isWifiEnabled_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.deviceMobileApp.platformSettings.signals.isWifiEnabled',
        "f26_gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isLocationEnabled_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.deviceMobileApp.platformSettings.signals.isLocationEnabled',
        "f28_gemaltoRiskEngine_attributes_deviceMobileApp_platform_signals_family_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.deviceMobileApp.platform.signals.family',
        "version_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.deviceMobileApp.platform.signals.version',
        "isRooted0_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.deviceMobileApp.platform.signals.isRooted',
        "f24_gemaltoRiskEngine_attributes_deviceMobileApp_network_signals_type_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.deviceMobileApp.network.signals.type',
        "f23_gemaltoRiskEngine_attributes_deviceMobileApp_network_signals_hwAddress_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.deviceMobileApp.network.signals.hwAddress',
        "f19_gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_fingerprint_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.deviceMobileApp.device.signals.fingerprint',
        "f20_gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_manufacturer_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.deviceMobileApp.device.signals.manufacturer',
        "f21_gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_model_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.deviceMobileApp.device.signals.model',
        "userId0_PATH" '$.context.userId',
        "f02_context_events_PATH" '$.context.events',
        "f05_context_usergroups_PATH" '$.context.usergroups',
        "customerGroupID_PATH" '$.context.customerGroupID',
        "f01_context_client_PATH" '$.context.client',
        "additionalNotes_PATH" '$.context.additionalNotes',
        "access_PATH" '$.decision.access',
        "f25_gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isBluetoothEnabled_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.deviceMobileApp.platformSettings.signals.isBluetoothEnabled',
        "time0_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.deviceMobileApp.location.signals.time',
        "f22_gemaltoRiskEngine_attributes_deviceMobileApp_location_signals_source_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.deviceMobileApp.location.signals.source',
        "latitude0_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.deviceMobileApp.location.signals.latitude',
        "longitude0_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.deviceMobileApp.location.signals.longitude',
        "accuracy_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.deviceMobileApp.location.signals.accuracy',
        "altitude_PATH" '$.getDecisionDetails.gemaltoRiskEngine[0:].attributes.deviceMobileApp.location.signals.altitude',
        "f04_context_flag002_PATH" '$.context.flag002',
        "f03_context_flag001_PATH" '$.context.flag001',

        "DIRECTORY" '/home/sqlstream/fpp-dev/data',
        "FILENAME_PATTERN" 'sample.*\.json'

);

-- add the missing feature columns
CREATE OR REPLACE VIEW "sample_json_ingest"."sample_json_in" 
AS SELECT STREAM * 
, 600 AS "f16_gemaltoRiskEngine_attributes_deviceBrowser_screenHeight"
, 250 AS  "f17_gemaltoRiskEngine_attributes_deviceBrowser_screenWidth"
, '111' AS "f29_gemaltoRiskEngine_attributes_ipintelf29ligence_ipinfo_Location_CityData_area_code"
, 'NULL' AS "f52_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ProxyData_proxy_level"
, 'NULL' AS "f53_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ProxyData_proxy_type"
, 'NULL' AS "f54_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_anonymizer_status"
FROM "sample_json_ingest"."sample_json_in_fs";

-- For development, throttle the data

CREATE OR REPLACE FUNCTION "throttle"(inputRows CURSOR, throttleScale INTEGER)
    returns TABLE(inputRows.*)
LANGUAGE JAVA
PARAMETER STYLE SYSTEM DEFINED JAVA
NO SQL
EXTERNAL NAME 'class:com.sqlstream.plugin.timesync.ThrottleStream.throttle';

CREATE OR REPLACE VIEW "sample_json_ingest"."sample_throttled_data"
AS
SELECT STREAM * 
FROM STREAM
    ( "throttle"
        ( CURSOR(SELECT STREAM * FROM "sample_json_in")
        , 1
        )
    );

-- use throttled or unthrottled source for this view

CREATE OR REPLACE VIEW "sample_json_ingest"."sample_json_with_times"
AS
SELECT STREAM * 
     , TIMESTAMP_TO_CHAR('u',"txn_time")  as "f06_day_of_week"
     , TIMESTAMP_TO_CHAR('H',"txn_time")  as "f56_hour_of_day"
FROM (
    SELECT STREAM *, TO_TIMESTAMP("time") as "txn_time"
    --FROM "sample_json_ingest"."sample_throttled_data"
    FROM "sample_json_ingest"."sample_json_in"
);

