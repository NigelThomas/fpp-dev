#!/bin/bash
GIT_ACCOUNT=https://github.com/NigelThomas
GIT_PROJECT_NAME=fpp-dev
BASE_IMAGE=sqlstream/streamlab-git
: ${BASE_IMAGE_LABEL:=latest}

: ${CONTAINER_NAME:=fpp-dev}

docker kill $CONTAINER_NAME
docker rm $CONTAINER_NAME

CONTAINER_DATA_SOURCE=/home/sqlstream/fpp-data
CONTAINER_DATA_TARGET=/home/sqlstream/output
CONTAINER_JNDI_DIR=/home/sqlstream/jndi

# Unless disabled, link the target volume
# LEAVE AS ALWAYS DISABLED - local output stays on the container only

# mount the custom JNDI directory if needed (else we use the git repo's jndi directory
# note: you may use the project's own jndi directory in which case working copies of properties files will override committed/pushed copies

if [ -n "$HOST_JNDI_DIR" ]
then
    HOST_JNDI_MOUNT="-v ${HOST_JNDI_DIR:=$HERE/jndi}:$CONTAINER_JNDI_DIR"
fi

docker run -v ${HOST_DATA_SOURCE:=$HOME/fpp-data}:$CONTAINER_DATA_SOURCE $HOST_TGT_MOUNT $HOST_JNDI_MOUNT \
           -e GIT_ACCOUNT=$GIT_ACCOUNT -e GIT_PROJECT_NAME=$GIT_PROJECT_NAME -e GIT_PROJECT_HASH=$GIT_PROJECT_HASH \
           -e LOAD_SLAB_FILES= \
           -e SQLSTREAM_HEAP_MEMORY=${SQLSTREAM_HEAP_MEMORY:=4096m} \
           -e SQLSTREAM_SLEEP_SECS=${SQLSTREAM_SLEEP_SECS:=10} \
           -d --name $CONTAINER_NAME -it $BASE_IMAGE:$BASE_IMAGE_LABEL

docker logs -f $CONTAINER_NAME
