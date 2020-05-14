-- scoring_pipeline.sql
--
-- Takes the result of the feature_engineering.sql pipeline and calls DSS
--
-- TODO add this to test_pipeline.sql 

CREATE OR REPLACE SCHEMA "scoring";

SET SCHEMA '"scoring"';
SET PATH '"scoring"';

!set force on
DROP FUNCTION "HttpPost" CASCADE;
DROP JAR "sqlstream-http" OPTIONS(0);
!set force off

-- create a jar for the HttpPost UDX
-- the Java code can be found in https://github.com/NigelThomas/sqlstream-http.git

create or replace jar "sqlstream-http"
    LIBRARY 'file:/home/sqlstream/fpp-dev/sqlstream-http.jar'
    OPTIONS(0);


-- create a function for the HttpPost UDX

CREATE OR REPLACE FUNCTION "HttpPost"
( inputRows CURSOR
, optionRows CURSOR                         
, "postDataColumnName" VARCHAR(128)
) RETURNS TABLE
( inputRows.*
, "httpResult" INTEGER
, "httpResponse" VARCHAR(1000)
)
LANGUAGE JAVA
PARAMETER STYLE SYSTEM DEFINED JAVA
NO SQL
EXTERNAL NAME '"scoring"."sqlstream-http":com.sqlstream.utilities.http.HttpPost.httpPost';

-- add required wrapper to the JSON feature array

CREATE OR REPLACE VIEW "scoring_step_110" 
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
, '{ "input_features": ' || 
  "features_1140"|| 
  ', "model_id": "" }' 
  as "post_data"
FROM "interface"."fe_pipeline_out";

-- create a view with the OPTIONS
-- TODO: externalize to a flat file so we can have different options in test and prod

CREATE OR REPLACE VIEW "HttpPostOptions"
( "option", "value")
AS 
SELECT * FROM 
(VALUES('URL', 'https://fpp-private.cto-dev.acloud.gemalto.com/public/api/v1/fppk/getscore/run')
      ,('DO-POST', 'FALSE')
      ,('DO-PASSTHROUGH', 'TRUE')
      ,('DUMMY-RESPONSE','{"response":{"result":0.421875,"error":""},"timing":{"preProcessing":0,"wait":17,"execution":133720,"functionInternal":132803},"apiContext":{"serviceId":"fppk","endpointId":"getscore","serviceGeneration":"v6"}}')
);  


-- create a view that calls the HttpPost UDX and returns the score

CREATE OR REPLACE VIEW "scoring_step_120" 
AS
SELECT STREAM * 
FROM STREAM 
    ("HttpPost"
        ( CURSOR (SELECT STREAM * FROM "scoring_step_110")
        , CURSOR (SELECT * FROM "HttpPostOptions")
        , 'post_data'
        )
    );

-- Now parse out the score from the httpResponse (assuming the httpResult is good)

-- This is an example of the response format:
-- {
-- 	"response": {
-- 		"result": 0.421875,
-- 		"error": ""
-- 	},
-- 	"timing": {
-- 		"preProcessing": 0,
-- 		"wait": 17,
-- 		"execution": 133720,
-- 		"functionInternal": 132803
-- 	},
-- 	"apiContext": {
-- 		"serviceId": "fppk",
-- 		"endpointId": "getscore",
-- 		"serviceGeneration": "v6"
-- 	}
-- }

CREATE OR REPLACE FUNCTION "parseDssResponse"(
   input cursor,
   columnName select from input, 
   parserClassName varchar(256),
   options cursor
)
returns table
(    input.*
    ,"result" REAL
    ,"error"  VARCHAR(250)
    ,"preProcessing" BIGINT
    ,"wait" BIGINT
    ,"execution" BIGINT
    ,"functionInternal" BIGINT
)
language java
parameter style system defined java
no sql
external name 'class com.sqlstream.aspen.namespace.common.ParserUdx.parseColumn';

-- a view of JSON parser options

CREATE OR REPLACE VIEW "parsingOptions"
( "ROW_PATH", "result_PATH", "error_PATH", "preProcessing_PATH", "wait_PATH", "execution_PATH", "functionInternal_PATH")
AS
SELECT * FROM (
    VALUES('$','$.response.result','$.response.error','$.timing.preProcessing','$.timing.wait','$.timing.execution','$.timing.functionInternal')

);

CREATE OR REPLACE VIEW "scoring_step_130" AS
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
, "post_data"     
, "httpResult"  
, "httpResponse"    
, JSON."result"
, JSON."error"
, JSON."preProcessing"
, JSON."wait"
, JSON."execution"
, JSON."functionInternal"
FROM STREAM 
        ("parseDssResponse"
            ( CURSOR(SELECT STREAM * FROM "scoring_step_120")
            , ROW("httpResponse")
            , 'JSON'
            , CURSOR(SELECT * FROM "parsingOptions")
        )) JSON;

-- pump to output stream        

CREATE OR REPLACE PUMP "interface"."scoring_pipeline_out_pump" STOPPED
AS
INSERT INTO "interface"."scoring_pipeline_out"
( "transaction_id" 
, "tenantId"
, "signals" 
, "headers" 
, "neustar" 
, "eval" 
, "score" 
, "user_id"       
, "device_id"     
, "post_data"    
, "httpResult"   
, "httpResponse"  
, "result"       
, "error"          
, "preProcessing"  
, "wait"          
, "execution"     
, "functionInternal"  
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
, "post_data"    
, "httpResult"   
, "httpResponse"  
, "result"       
, "error"          
, "preProcessing"  
, "wait"          
, "execution"     
, "functionInternal"  
FROM "scoring"."scoring_step_130";

