-- dump_input.sql
-- 
-- Take the parsed input from "interface"."transactions" and write it to a file (for comparison with the python model)

SET SCHEMA '"feature_engineering"';

CREATE OR REPLACE FOREIGN STREAM "dump_transactions"
( "transaction_time" VARCHAR(24)
, "transaction_id" varchar(64)
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
, "gemaltoRiskEngine_attributes_deviceBrowser_userAgent" varchar(64)
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
)
SERVER FILE_SERVER
OPTIONS
( FORMATTER 'CSV'
, SEPARATOR '|'
, FILE_ROTATION_SIZE '1g'
, ORIGINAL_FILENAME 'tmp-txns-out.csv'
, FILENAME_PREFIX 'txns_out'
, DIRECTORY '/home/sqlstream/output'
, WRITE_HEADER 'false'
, FILENAME_SUFFIX '.csv'
, FORMATTER_INCLUDE_ROWTIME 'false'
);

CREATE OR REPLACE PUMP "interface"."dump_transactions_pump" STOPPED
AS
INSERT INTO "dump_transactions"
( "transaction_time"
, "transaction_id"
, "user_id"
, "device_id"
, "city_cf" 
, "country_cf"
, "state_cf"
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
  timestamp_to_char('yyyy-MM-dd HH:mm:ss',s.ROWTIME)
, "transaction_id"
, "user_id"
, "device_id"
, "city_cf" 
, "country_cf"
, "state_cf"
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
FROM "interface"."transactions" s;


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