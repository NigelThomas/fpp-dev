#!/bin/bash
IMAGE=sqlstream/streamlab-git
: ${BASE_IMAGE_LABEL:=release}

: ${CONTAINER:=fpp-dev}

: ${SAMPLE_DATA:=$HOME/fpp-data}

# Start the fpp-dev demo image
docker kill ${CONTAINER}
docker rm ${CONTAINER}
docker run -p 80:80 -p 5560:5560 -p 5580:5580 -p 5595:5595 -e PROJECT_NAME=fpp-dev \
    -v ${SAMPLE_DATA}:/home/sqlstream/fpp-data \
    -d --name ${CONTAINER} -it ${IMAGE}:${BASE_IMAGE_LABEL}
docker logs -f ${CONTAINER}

