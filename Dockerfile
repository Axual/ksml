FROM redhat/ubi8:8.4-206.1626828523
MAINTAINER Axual <maintainer@axual.io>
ENV JAVA_HOME=/opt/graalvm
ENV PATH=/opt/graalvm/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

RUN mkdir -p "/opt/ksml/libs"  \
&& curl -k -L -o "/tmp/graalvm.tgz" "https://github.com/graalvm/graalvm-ce-builds/releases/download/vm-21.2.0/graalvm-ce-java11-linux-amd64-21.2.0.tar.gz" \
&& tar -xzf /tmp/graalvm.tgz -C "/opt" \
&& mv /opt/graalvm* /opt/graalvm \
&& chown -R 1024:users /opt \
&& rm -rf /tmp/graalvm.tgz \
&& /opt/graalvm/bin/gu -A install python

ADD --chown=1024:users target/libs/ /opt/ksml/libs/
ADD --chown=1024:users target/ksml-runner*.jar /opt/ksml/ksml.jar

WORKDIR /opt/ksml
USER 1024
ENTRYPOINT ["java", "-jar", "/opt/ksml/ksml.jar"]
