#!/bin/bash
#
# Set up environment for someone who has exec'd onto the docker container

. /etc/sqlstream/environment
export GIT_PROJECT_NAME=fpp-dev
export SQLSTREAM_HOME
export PATH=/home/sqlstream/bin:/home/sqlstream/${GIT_PROJECT_NAME}:/home/sqlstream/sqlstream-docker-utils:$PATH:$SQLSTREAM_HOME/bin
. /home/sqlstream/sqlstream-docker-utils/serviceFunctions.sh
