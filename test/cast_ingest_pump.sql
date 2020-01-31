-- cast_ingest_pump.sql
--
-- Assumes interface_transactions columns are all VARCHAR(64)

CREATE OR REPLACE PUMP "interface"."ingest_pump" STOPPED 
AS 
INSERT INTO "interface"."transactions"
( ROWTIME
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
  "txn_time" AS ROWTIME      -- promote to ROWTIME
, "f57_visitId"        -- identifies the transaction 
, "userId"               -- used for windows
, "f19_gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_fingerprint"             -- used for windows = deviceId (at least for mobile)
, CAST("city_cf" AS VARCHAR(8))               -- locations come with confidence
, CAST("country_cf" AS VARCHAR(8))
, CAST("state_cf" AS VARCHAR(8))
, "f01_context_client"                        -- from here, the features, in alphabetical order of path
, "f02_context_events"
, CAST("f03_context_flag001" AS VARCHAR(8))
, CAST("f04_context_flag002" AS VARCHAR(8))
, "f05_context_usergroups"
, "f06_day_of_week" 
, "f07_gemaltoRiskEngine_attributes_deviceBrowser_browserName"
, "f08_gemaltoRiskEngine_attributes_deviceBrowser_browserVersion"
, CAST("f09_gemaltoRiskEngine_attributes_deviceBrowser_isRooted" AS VARCHAR(8))
, "f10_gemaltoRiskEngine_attributes_deviceBrowser_networkIp"
, CAST("f11_gemaltoRiskEngine_attributes_deviceBrowser_osFamily" AS VARCHAR(8))
, "f12_gemaltoRiskEngine_attributes_deviceBrowser_osName"
, CAST("f13_gemaltoRiskEngine_attributes_deviceBrowser_osReleaseDateOrder" AS VARCHAR(8))
, CAST("f14_gemaltoRiskEngine_attributes_deviceBrowser_osVersionOfFamily" AS VARCHAR(64))
, "f15_gemaltoRiskEngine_attributes_deviceBrowser_osVersion"
, CAST("f16_gemaltoRiskEngine_attributes_deviceBrowser_screenHeight" AS VARCHAR(8))
, CAST("f17_gemaltoRiskEngine_attributes_deviceBrowser_screenWidth" AS VARCHAR(8))
, "f18_gemaltoRiskEngine_attributes_deviceBrowser_userAgent"
, "f19_gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_fingerprint"
, "f20_gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_manufacturer"
, "f21_gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_model"
, "f22_gemaltoRiskEngine_attributes_deviceMobileApp_location_signals_source"
, "f23_gemaltoRiskEngine_attributes_deviceMobileApp_network_signals_hwAddress"
, "f24_gemaltoRiskEngine_attributes_deviceMobileApp_network_signals_type"
, CAST("f25_gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isBluetoothEnabled" AS VARCHAR(8))
, CAST("f26_gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isLocationEnabled" AS VARCHAR(8))
, CAST("f27_gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isWifiEnabled" AS VARCHAR(8))
, "f28_gemaltoRiskEngine_attributes_deviceMobileApp_platform_signals_family"
, CAST("f29_gemaltoRiskEngine_attributes_ipintelf29ligence_ipinfo_Location_CityData_area_code" AS VARCHAR(8))
, "f30_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_city"
, "f31_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_postal_code"
, CAST("f32_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_time_zone" AS VARCHAR(64))
, "f33_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country_code"
, "f34_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country"
, "f35_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state_code"
, "f36_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state"
, "f37_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_continent"
, CAST("f38_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_dma" AS VARCHAR(64))
, CAST("f39_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_latitude" AS VARCHAR(64))
, CAST("f40_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_longitude" AS VARCHAR(64))
, CAST("f41_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_msa" AS VARCHAR(64))
, "f42_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_region"
, "f43_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_Domain_sld"
, "f44_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_Domain_tld"
, CAST("f45_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_asn" as VARCHAR(64))
, "f46_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_carrier"
, "f47_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_connection_type"
, CAST("f48_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_hosting_facility" AS VARCHAR(8))
, "f49_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_ip_routing_type"
, "f50_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_line_speed"
, "f51_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_organization"
, CAST("f52_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ProxyData_proxy_level" AS VARCHAR(64))
, "f53_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ProxyData_proxy_type" 
, "f54_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_anonymizer_status"
, "f55_gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ip_type"
, "f56_hour_of_day" 
, "f57_visitId"
FROM "sample_json_ingest"."sample_json_with_times" s
WHERE "rulesetExternalId" = 'pilot_1'            -- we only want one set of data for each txn
ORDER BY "txn_time" WITHIN INTERVAL '10' SECOND
;
