#!/bin/bash

echo "Starting FPP SQLStream pipeline..."

export SQLSTREAM_HOME="/opt/sqlstream/6.0.0.18631/s-Server"
export SQLSTREAM_FPP="/home/sqlstream/fpp"

service s-serverd start
service webagentd start

if [ -z "$STARTUP_TIMEOUT" ]; then
    STARTUP_TIMEOUT=120
fi

count=$STARTUP_TIMEOUT
status=1
until [ $count -eq 0 ] || $SQLSTREAM_HOME/bin/serverReady --quiet; do
  count=$(($count - 1))
  if [ $count -ne 0 ]; then
    sleep 1
  fi
done

if $SQLSTREAM_HOME/bin/serverReady --quiet; then
  echo "Provisionning FPP SQLStream pipeline..."
  $SQLSTREAM_HOME/bin/sqllineClient --run=$SQLSTREAM_FPP/sql/fpp-pipeline.sql
else
   echo "FATAL: s-Server not ready!"
fi

tail -F /var/log/sqlstream/Trace.log.0

echo "FPP SQLStream pipeline started."
