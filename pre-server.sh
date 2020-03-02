#!/bin/bash
#
# pre-server.sh
#
# Run by the sqlstream/streamlab-git docker image before s-server is started

mkdir -p /home/sqlstream/output
chown sqlstream:sqlstream /home/sqlstream/output



