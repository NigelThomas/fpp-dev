FROM dockerhub.gemalto.com/risk-engine/sqlstream/bps-fpp-minimal:1.0.0

COPY --chown=sqlstream fpp /home/sqlstream/fpp
COPY --chown=sqlstream json-path-2.3.0-with-dependencies.tar.gz  /home/sqlstream

WORKDIR /home/sqlstream
RUN gunzip json-path-2.3.0-with-dependencies.tar.gz \
&& tar -xvf json-path-2.3.0-with-dependencies.tar

# build udx jar
WORKDIR fpp/java/src
RUN jar xf ../../../lib/accessors-smart-1.2.jar \
&& jar xf ../../../lib/asm-5.0.4.jar \
&& jar xf ../../../lib/json-path-2.3.0.jar \
&& jar xf ../../../lib/json-smart-2.3.jar \
&& jar xf ../../../lib/slf4j-api-1.7.25.jar \
&& jar xf ../../../lib/stax-api-1.0.1.jar \
&& javac $(find com/sqlstream/udx/* | grep .java) \
&& jar cf /home/sqlstream/fpp/fpp-udx.jar *
WORKDIR ../..

# replace embedded license with the Gemalto license
RUN rm /opt/sqlstream/6.0.0.18631/s-Server/ECaTS911-License-v6-2020.lic
COPY --chown=sqlstream license/GemaltoLicense-2020-03-31.lic /opt/sqlstream/6.0.0.18631/s-Server

RUN chmod +x /home/sqlstream/fpp/startup.sh
ENTRYPOINT [ "/home/sqlstream/fpp/startup.sh" ]