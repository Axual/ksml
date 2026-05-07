#!/bin/sh
JAVA_MAJOR=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d'.' -f1)
if [ "$JAVA_MAJOR" -lt 24 ]; then
    exec java -Djava.security.manager=allow -jar /opt/ksml/ksml.jar "$@"
else
    exec java -jar /opt/ksml/ksml.jar "$@"
fi
