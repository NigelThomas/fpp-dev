-- ---------------------------------------------------------------------------------------------------------------------
-- EXTERNAL STREAM SINK: scores
-- ---------------------------------------------------------------------------------------------------------------------

SET SCHEMA '"fpp-test-score-0.1_v2"';
SET PATH '"fpp-test-score-0.1_v2"';

-- Compute score
CREATE OR REPLACE FUNCTION "addScore" (c CURSOR)
    RETURNS TABLE( c.*, "score" VARCHAR(1024) )
    LANGUAGE JAVA PARAMETER STYLE SYSTEM DEFINED JAVA NO SQL EXTERNAL NAME 'fpp_udx:com.sqlstream.udx.UdxCallAPI.addScore';

CREATE OR REPLACE VIEW "scores" AS
    SELECT STREAM
        CAST("result"."transaction_id" AS VARCHAR(256)) AS "transaction_id",
        CAST("result"."user_id" AS VARCHAR(256)) AS "user_id",
        CAST("result"."device_id" AS VARCHAR(256)) AS "device_id",
        CAST("result"."features_1140" AS VARCHAR(32000)) AS "features_1140",
        CAST("result"."score" AS VARCHAR(1024)) AS "score",
        "transactions".*,
        "signals"."signals", "signals"."headers", "signals"."neustar",
        "contexts"."eval"
    FROM STREAM( "addScore"(CURSOR( SELECT STREAM * FROM "interface"."fe_pipeline_out"))) as "result"
    INNER JOIN "interface"."transactions" OVER (RANGE INTERVAL '1' HOUR PRECEDING) AS "transactions"
        ON ("transactions"."visitId" = "result"."transaction_id")
    INNER JOIN "fpp-test-score-0.1_v2"."signals" OVER (RANGE INTERVAL '1' HOUR PRECEDING) AS "signals"
        ON ("signals"."visitId" = "result"."transaction_id")
    INNER JOIN "fpp-test-score-0.1_v2"."contexts" OVER (RANGE INTERVAL '1' HOUR PRECEDING) AS "contexts"
        ON ("contexts"."visitId" = "result"."transaction_id");


-- Define a Kafka output stream to topic "scores"
CREATE OR REPLACE FOREIGN STREAM "scores_fs"
    (
        "transaction_id" VARCHAR(256)
        ,"user_id" VARCHAR(256)
        ,"device_id" VARCHAR(256)
        ,"features_1140" VARCHAR(32000)
        ,"score" VARCHAR(1024)
        ,"signals" VARCHAR(1024000)
        ,"headers" VARCHAR(1024000)
        ,"neustar" VARCHAR(1024000)
        ,"eval" VARCHAR(1024000)
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

    SERVER "KAFKA10_SERVER" OPTIONS (
        "FORMATTER" 'JSON',
        "FORMATTER_INCLUDE_ROWTIME" 'false',
        "TOPIC" 'scores',
        "kafka.producer.config" '/home/sqlstream/config/kafka.properties'
    );

-- pump scores -> scores_fs
CREATE OR REPLACE PUMP "scores-to-scores_fs-Pump" STOPPED AS INSERT INTO "scores_fs" SELECT STREAM
"transaction_id"
        ,"user_id"
        ,"device_id"
        ,"features_1140"
        ,"score"
        ,"signals"
        ,"headers"
        ,"neustar"
        ,"eval"
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
 FROM "scores";