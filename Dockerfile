FROM redhat/ubi8:8.6-990
MAINTAINER Axual <maintainer@axual.io>
ENV JAVA_HOME=/opt/graalvm
ENV PATH=/opt/graalvm/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

COPY --from=ghcr.io/graalvm/graalvm-ce:22.3.0 /usr/java/latest/ /opt/graalvm

RUN mkdir -p "/opt/ksml/libs"  \
&& chown -R 1024:users /opt \
&& /opt/graalvm/bin/gu -A install python

ADD --chown=1024:users target/libs/ /opt/ksml/libs/
ADD --chown=1024:users target/ksml-runner*.jar /opt/ksml/ksml.jar

WORKDIR /opt/ksml
USER 1024
ENTRYPOINT ["java", "-jar", "/opt/ksml/ksml.jar"]
