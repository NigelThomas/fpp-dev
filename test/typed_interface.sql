CREATE OR REPLACE SCHEMA "interface";
ALTER PUMP "interface".* stop;

DROP SCHEMA "interface" cascade;
CREATE OR REPLACE SCHEMA "interface";


CREATE OR REPLACE STREAM "interface"."transactions"
( "txnId" VARCHAR(32)
, "userId" VARCHAR(32)
, "deviceId" VARCHAR(64)
, "city_cf" INTEGER
, "country_cf" INTEGER
, "state_cf" INTEGER
, "context_client" VARCHAR(8)
, "context_events" VARCHAR(32)
, "context_flag001" BOOLEAN
, "context_flag002" BOOLEAN
, "context_usergroups" VARCHAR(16)
, "day_of_week" VARCHAR(2)
, "gemaltoRiskEngine_attributes_deviceBrowser_browserName" VARCHAR(16)
, "gemaltoRiskEngine_attributes_deviceBrowser_browserVersion" VARCHAR(16)
, "gemaltoRiskEngine_attributes_deviceBrowser_isRooted" BOOLEAN
, "gemaltoRiskEngine_attributes_deviceBrowser_networkIp" VARCHAR(64)
, "gemaltoRiskEngine_attributes_deviceBrowser_osFamily" VARCHAR(8)
, "gemaltoRiskEngine_attributes_deviceBrowser_osName" VARCHAR(8)
, "gemaltoRiskEngine_attributes_deviceBrowser_osReleaseDateOrder" INTEGER
, "gemaltoRiskEngine_attributes_deviceBrowser_osVersionOfFamily" DECIMAL(2,0)
, "gemaltoRiskEngine_attributes_deviceBrowser_osVersion" VARCHAR(8)
, "gemaltoRiskEngine_attributes_deviceBrowser_screenHeight" INTEGER
, "gemaltoRiskEngine_attributes_deviceBrowser_screenWidth" INTEGER
, "gemaltoRiskEngine_attributes_deviceBrowser_userAgent" VARCHAR(256)
, "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_fingerprint" VARCHAR(64)
, "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_manufacturer" VARCHAR(16)
, "gemaltoRiskEngine_attributes_deviceMobileApp_device_signals_model" VARCHAR(16)
, "gemaltoRiskEngine_attributes_deviceMobileApp_location_signals_source" VARCHAR(8)
, "gemaltoRiskEngine_attributes_deviceMobileApp_network_signals_hwAddress" VARCHAR(64)
, "gemaltoRiskEngine_attributes_deviceMobileApp_network_signals_type" VARCHAR(8)
, "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isBluetoothEnabled" BOOLEAN
, "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isLocationEnabled" BOOLEAN
, "gemaltoRiskEngine_attributes_deviceMobileApp_platformSettings_signals_isWifiEnabled" BOOLEAN
, "gemaltoRiskEngine_attributes_deviceMobileApp_platform_signals_family" VARCHAR(8)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_area_code" CHAR(3)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_city" VARCHAR(32)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_postal_code" VARCHAR(16)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CityData_time_zone" DECIMAL(3,1)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country_code" VARCHAR(4)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_CountryData_country" VARCHAR(16)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state_code" VARCHAR(4)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_StateData_state" VARCHAR(32)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_continent" VARCHAR(16)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_dma" INTEGER
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_latitude" REAL
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_longitude" DOUBLE
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_msa" BIGINT
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Location_region" VARCHAR(16)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_Domain_sld" VARCHAR(16)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_Domain_tld" VARCHAR(8)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_asn" INTEGER
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_carrier" VARCHAR(64)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_connection_type" VARCHAR(16)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_hosting_facility" BOOLEAN
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_ip_routing_type" VARCHAR(16)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_line_speed" VARCHAR(8)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_Network_organization" VARCHAR(64)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ProxyData_proxy_level" INTEGER
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ProxyData_proxy_type" VARCHAR(8)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_anonymizer_status" VARCHAR(8)
, "gemaltoRiskEngine_attributes_ipintelligence_ipinfo_ip_type" VARCHAR(8)
, "hour_of_day" VARCHAR(2)
, "visitId" VARCHAR(64)
);

