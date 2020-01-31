# Dockerfile

# Base the image on standard SQLstream image
FROM sqlstream/minimal:release

# We use git to get project code; vim for debugging, time for timing

RUN apt-get update &&\
    apt-get install -y git time vim-tiny

# These environment variables can be set when building the image
ARG PROJECT_NAME_ARG=fpp-dev
ENV PROJECT_NAME=${PROJECT_NAME_ARG}

WORKDIR /home/sqlstream

# the base image already EXPOSEs required ports - add EXPOSE here if you need to change, extend or reduce the list

# when the container starts, we want to start the project pumps and then tail the s-Server trace log
# note late binding to get latest version of sqlstream-docker-utils

ENTRYPOINT source /etc/sqlstream/environment &&\
           export PATH=/home/sqlstream/${PROJECT_NAME}:$SQLSTREAM_HOME/bin:$PATH &&\
           git clone --depth 1 https://github.com/NigelThomas/fpp-dev.git &&\
           cd /home/sqlstream/${PROJECT_NAME} &&\
           echo "Loading project ${PROJECT_NAME}" &&\
           PROJECT_NAME=${PROJECT_NAME} time startup.sh &&\
           tail -f /var/log/sqlstream/Trace.log.0

