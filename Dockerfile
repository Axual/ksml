# This Dockerfile expects build artifacts in the build-output/ directory.

# Targets:
# - base            = UBI image with ksml user and build packages from microdnf
# - graal           = base stage plus GraalVM installed
# - ksml            = base plus GraalVM and pre-built artifacts from build-output/

# Step 1: Create the common base image with the ksml user and group and the required packages
FROM registry.access.redhat.com/ubi9/ubi-minimal:9.7 AS base
ENV LANG=en_US.UTF-8

# Environment variable for Connect Build and Runtime
ENV PATH=/opt/graal/bin:$PATH \
	KSML_HOME="/home/ksml" \
    	KSML_INSTALL="/opt/ksml"

RUN set -eux \
    	&& microdnf upgrade -y --nodocs \
        && microdnf install -y --nodocs procps tar unzip gzip zlib openssl-devel gcc gcc-c++ \
    		    make patch glibc-langpack-en libxcrypt shadow-utils \
        && useradd -g 0 -u 1024 -d "$KSML_HOME" -ms /bin/sh -f -1 ksml \
        && chown -R ksml:0 "$KSML_HOME" /opt                               \
        && chmod -R g=u /home /opt

# Step 2: Download and install GraalVM on top of the base image of step 1
FROM base AS graal
ARG TARGETARCH
ARG GRAALVM_JDK_VERSION=23.0.2
RUN set -eux \
    && JAVA_ARCH= \
    && case "$TARGETARCH" in \
    amd64) \
    	JAVA_ARCH="x64" \
    ;; \
    arm64) \
    	JAVA_ARCH="aarch64" \
    ;; \
    *) \
    	echo "Unsupported target architecture $TARGETARCH" \
    	exit 1 \
    ;; \
    esac  \
    && export GRAALVM_PKG=https://github.com/graalvm/graalvm-ce-builds/releases/download/jdk-${GRAALVM_JDK_VERSION}/graalvm-community-jdk-${GRAALVM_JDK_VERSION}_linux-${JAVA_ARCH}_bin.tar.gz \
    && mkdir -p /opt/graal \
    && curl --fail --silent --location --retry 3 ${GRAALVM_PKG} | gunzip | tar x -C /opt/graal --strip-components=1

# Step 3: Use the base image and copy GraalVM from Step 2 and pre-built artifacts from build-output/
FROM base AS ksml
LABEL io.axual.ksml.authors="maintainer@axual.io"
ENV JAVA_HOME=/opt/graalvm
COPY --chown=ksml:0 --from=graal /opt/graal/ /opt/graal/

WORKDIR /home/ksml
USER 1024

# Copy pre-built artifacts from GitHub Actions (expected in build-output/ directory)
COPY --chown=ksml:0 build-output/NOTICE.txt /licenses/THIRD-PARTY-LICENSES.txt
COPY --chown=ksml:0 build-output/LICENSE.txt /licenses/LICENSE.txt
COPY --chown=ksml:0 build-output/libs/ /opt/ksml/libs/
COPY --chown=ksml:0 build-output/ksml-runner*.jar /opt/ksml/ksml.jar

ENTRYPOINT ["java", "-Djava.security.manager=allow", "-jar", "/opt/ksml/ksml.jar"]
