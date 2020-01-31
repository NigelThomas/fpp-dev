#!/bin/bash
docker kill fpp-dev
docker rm fpp-dev
docker build -t nigelclthomas/fpp-dev --build-arg PROJECT_NAME_ARG=fpp-dev .
