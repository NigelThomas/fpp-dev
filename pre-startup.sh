#!/bin/bash
#
# pre-startup.sh
#
# Run by the sqlstream/streamlab-git docker image as soon as s-server has been started

. /etc/sqlstream/environment

# This test project depends on data files that have been mounted to /home/sqlstream/fpp-data

echo ... load the test schema 
$SQLSTREAM_HOME/bin/sqllineClient --run=test_pipeline.sql




