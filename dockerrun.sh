#!/bin/bash

# Start the fpp-dev demo image
docker kill fpp-dev
docker rm fpp-dev
docker run -p 80:80 -p 5560:5560 -p 5580:5580 -p 5595:5595 -e PROJECT_NAME=fpp-dev -d --name fpp-dev -it nigelclthomas/fpp-dev
docker logs -f fpp-dev

