CREATE OR REPLACE SCHEMA "fpp-test-score-0.1";
ALTER PUMP "fpp-test-score-0.1".* STOP;
DROP SCHEMA "fpp-test-score-0.1" CASCADE;
CREATE OR REPLACE SCHEMA "fpp-test-score-0.1";

-- ---------------------------------------------------------------------------------------------------------------------
-- EXTERNAL STREAM SOURCE: signals
-- ---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FOREIGN STREAM "fpp-test-score-0.1"."signals_raw_fs"
(
    "tenantId" VARCHAR(256),
    "visitId" VARCHAR(64),
    "signals" VARCHAR(102400),
    "headers" VARCHAR(102400),
    "neustar" VARCHAR(102400)
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
        "STARTING_TIME" 'LATEST',
        "MAX_POLL_RECORDS" '100',
        "STARTING_OFFSET" '-1',
        "BUFFER_SIZE" '1048576',
        "FETCH_SIZE" '1000000',
        "CLIENT_ID" 'fpp-test-score-signals',
        "isolation.level" 'read_uncommitted',
        "TOPIC" 'signals'
    );

CREATE OR REPLACE STREAM "fpp-test-score-0.1"."signals"
(
    "tenantId" VARCHAR(256),
    "visitId" VARCHAR(64),
    "signals" VARCHAR(102400),
    "headers" VARCHAR(102400),
    "neustar" VARCHAR(102400)
);

CREATE OR REPLACE PUMP "fpp-test-score-0.1"."source-to-signals-Pump" STOPPED AS
INSERT INTO "fpp-test-score-0.1"."signals"
SELECT STREAM * FROM "fpp-test-score-0.1"."signals_raw_fs";




-- ---------------------------------------------------------------------------------------------------------------------
-- EXTERNAL STREAM SOURCE: contexts
-- ---------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE FOREIGN STREAM "fpp-test-score-0.1"."contexts_raw_fs"
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
        "CLIENT_ID" 'fpp-test-score-contexts',
        "isolation.level" 'read_uncommitted',
        "TOPIC" 'contexts'
    );

CREATE OR REPLACE STREAM "fpp-test-score-0.1"."contexts"
(
    "visitId" VARCHAR(64),
    "eval" VARCHAR(102400)
);

CREATE OR REPLACE PUMP "fpp-test-score-0.1"."source-to-contexts-Pump" STOPPED AS
INSERT INTO "fpp-test-score-0.1"."contexts"
SELECT STREAM * FROM "fpp-test-score-0.1"."contexts_raw_fs";




-- ---------------------------------------------------------------------------------------------------------------------
-- EXTERNAL STREAM SINK: scores
-- ---------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE FOREIGN STREAM "fpp-test-score-0.1"."scores_fs"
(
    "tenantId" VARCHAR(256),
    "signals" VARCHAR(102400),
    "headers" VARCHAR(102400),
    "neustar" VARCHAR(102400),
    "eval" VARCHAR(102400),
    "score" VARCHAR(1024)
)
    SERVER "KAFKA10_SERVER"
    OPTIONS (
        "FORMATTER" 'JSON',
        "kafka.producer.config" '/home/sqlstream/config/kafka.properties',
        "TRANSACTION_ROWTIME_LIMIT" '1000',
        "pump.name" 'LOCALDB.fpp-test-score-0.1.scores_fs_pump',
        "HA_ROLLOVER_TIMEOUT" '5000',
        "compression.codec" 'none',
        "message.send.max.retries" '3',
        "retry.backoff.ms" '100',
        "request.timeout.ms" '10000',
        "topic.metadata.refresh.interval.ms" '600000',
        "send.buffer.bytes" '102400',
        "client.id" 'sqlstream',
        "POLL_TIMEOUT" '100',
        "producer.type" 'async',
        "queue.buffering.max.ms" '5000',
        "queue.buffering.max.messages" '1000',
        "queue.enqueue.timeout.ms" '-1',
        "batch.num.messages" '200',
        "TOPIC" 'scores'
    );

CREATE OR REPLACE STREAM "fpp-test-score-0.1"."scores"
(
    "tenantId" VARCHAR(256),
    "signals" VARCHAR(102400),
    "headers" VARCHAR(102400),
    "neustar" VARCHAR(102400),
    "eval" VARCHAR(102400),
    "score" VARCHAR(1024)
);

CREATE OR REPLACE PUMP "fpp-test-score-0.1"."scores_fs_pump" STOPPED AS
INSERT INTO "fpp-test-score-0.1"."scores_fs"
SELECT STREAM * FROM "fpp-test-score-0.1"."scores";



-- ---------------------------------------------------------------------------------------------------------------------
-- PIPELINE: scoring
-- ---------------------------------------------------------------------------------------------------------------------

--  Join contexts and signals / visitId
CREATE OR REPLACE VIEW "fpp-test-score-0.1"."scoring_step_1" AS
    SELECT STREAM "signals".*, "contexts"."eval"
    FROM "fpp-test-score-0.1"."contexts" AS "contexts"
    INNER JOIN "fpp-test-score-0.1"."signals" OVER (RANGE INTERVAL '1' HOUR PRECEDING) AS "signals"
        ON ("contexts"."visitId" = "signals"."visitId")  ;

--  Add parameters used for features computation
CREATE OR REPLACE JAR "fpp-test-score-0.1".fpp_udx LIBRARY 'file:/home/sqlstream/fpp/fpp-udx.jar' OPTIONS (0);
CREATE OR REPLACE FUNCTION "fpp-test-score-0.1"."extractProperties"(c CURSOR) RETURNS TABLE( c.*, "userId" VARCHAR(64), "browserName" VARCHAR(64), "osName" VARCHAR(64), "networkIp" VARCHAR(64) ) LANGUAGE JAVA PARAMETER STYLE SYSTEM DEFINED JAVA NO SQL EXTERNAL NAME '"fpp-test-score-0.1".fpp_udx:com.sqlstream.udx.UdxCallAPI.addFeatureParams';
CREATE OR REPLACE VIEW "fpp-test-score-0.1"."scoring_step_2" AS
    SELECT STREAM *
    FROM STREAM("fpp-test-score-0.1"."extractProperties"(CURSOR( SELECT STREAM * FROM "fpp-test-score-0.1"."scoring_step_1" AS "input")));


CREATE OR REPLACE VIEW "fpp-test-score-0.1"."scoring_feature_w24h_userid" AS
  SELECT STREAM *, COUNT("userId") OVER "window24h" AS "w24h_userId_count", COUNT(DISTINCT "browserName") OVER "window24h" AS "w24h_browserName_count", COUNT(DISTINCT "networkIp") OVER "window24h" AS "w24h_networkIp_count", COUNT(DISTINCT "osName") OVER "window24h" AS "w24h_osName_count"
  FROM "fpp-test-score-0.1"."scoring_step_2" AS "input"
  WINDOW "window24h" AS (PARTITION by "userId" RANGE INTERVAL '24' HOUR PRECEDING);

CREATE OR REPLACE VIEW "fpp-test-score-0.1"."scoring_feature_w6h_userid" AS
  SELECT STREAM *, COUNT("userId") OVER "window6h" AS "w6h_userId_count", COUNT(DISTINCT "browserName") OVER "window6h" AS "w6h_browserName_count", COUNT(DISTINCT "networkIp") OVER "window6h" AS "w6h_networkIp_count", COUNT(DISTINCT "osName") OVER "window6h" AS "w6h_osName_count"
  FROM "fpp-test-score-0.1"."scoring_feature_w24h_userid" AS "input"
  WINDOW "window6h" AS (PARTITION by "userId" RANGE INTERVAL '6' HOUR PRECEDING);

CREATE OR REPLACE VIEW "fpp-test-score-0.1"."scoring_feature_w1h_userid" AS
  SELECT STREAM *, COUNT("userId") OVER "window1h" AS "w1h_userId_count", COUNT(DISTINCT "browserName") OVER "window1h" AS "w1h_browserName_count", COUNT(DISTINCT "networkIp") OVER "window1h" AS "w1h_networkIp_count", COUNT(DISTINCT "osName") OVER "window1h" AS "w1h_osName_count"
  FROM "fpp-test-score-0.1"."scoring_feature_w6h_userid" AS "input"
  WINDOW "window1h" AS (PARTITION by "userId" RANGE INTERVAL '1' HOUR PRECEDING);

-- Compute score
CREATE OR REPLACE FUNCTION "fpp-test-score-0.1"."addScore"(c CURSOR) RETURNS TABLE( c.*, "score" VARCHAR(1024) ) LANGUAGE JAVA PARAMETER STYLE SYSTEM DEFINED JAVA NO SQL EXTERNAL NAME '"fpp-test-score-0.1".fpp_udx:com.sqlstream.udx.UdxCallAPI.addScore';
CREATE OR REPLACE VIEW "fpp-test-score-0.1"."scoring_step_30" AS
    SELECT STREAM  *
    FROM STREAM("fpp-test-score-0.1"."addScore"(CURSOR( SELECT STREAM * FROM "fpp-test-score-0.1"."scoring_feature_w1h_userid" AS "input")));



--  Define the pump
CREATE OR REPLACE PUMP "fpp-test-score-0.1"."scoring_step_40-to-scores-Pump" STOPPED AS
INSERT INTO "fpp-test-score-0.1"."scores"
    ("tenantId", "signals", "headers", "neustar", "eval", "score")
SELECT STREAM CAST("tenantId" AS VARCHAR(256)) AS "tenantId", CAST("signals" AS VARCHAR(102400)) AS "signals", CAST("headers" AS VARCHAR(102400)) AS "headers", CAST("neustar" AS VARCHAR(102400)) AS "neustar", CAST("eval" AS VARCHAR(102400)) AS "eval", CAST("score" AS VARCHAR(1024)) AS "score"
    FROM "fpp-test-score-0.1"."scoring_step_30" AS "input";
CREATE OR REPLACE VIEW "fpp-test-score-0.1"."scoring_out" AS SELECT STREAM * FROM "fpp-test-score-0.1"."scoring_step_30";


ALTER PUMP "fpp-test-score-0.1".* START;