#!/bin/sh
JAVA_MAJOR=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d'.' -f1)
if [ "$JAVA_MAJOR" -lt 24 ]; then
    exec java -Djava.security.manager=allow -jar /opt/ksml/ksml.jar "$@"
else
    # JDK 24+: grant native access (Truffle/GraalPy load native libs) and allow sun.misc.Unsafe
    # memory access (protobuf, Truffle). Without these the JVM prints "restricted method" and
    # "terminally deprecated Unsafe" warnings at startup.
    exec java --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow -jar /opt/ksml/ksml.jar "$@"
fi
