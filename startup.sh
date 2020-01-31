#!/bin/bash
#
# startup.sh
#
# extracts SQL and dashboards from one or more slab files and installs into s-Server
# assume PATH is set to ensure we can find subsidiary scripts
# assume cwd is set to the project directory

. serviceFunctions.sh

mkdir -p /home/sqlstream/output
chown sqlstream:sqlstream /home/sqlstream/output

# This test project depends on local data files
cd /home/sqlstream/${PROJECT_NAME}/data
gunzip sample*.gz
cd ..

# start s-Server to load the schema
startsServer

echo ... load the test schema and start pumps
sqllineClient --run=test_pipeline.sql


# now the caller ENTRYPOINT should tail the s-Server trace file forever – so this entrypoint never finishes
# and the trace file can be viewed using docker logs


