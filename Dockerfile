# Dockerfile for Axual Connect
# Local Build (ARM64/Apple Mx) docker buildx build --platform=linux/arm64 -t axual/ksml:local --load --target ksml .
# Local Build (AMD65) docker buildx build --platform=linux/amd64 -t axual/ksml:local --load --target ksml .
# Targets
# - base            = UBI image with ksml user and build packages from microdnf
# - graal-builder   = base stage plus Maven and GraalVM and GraalPython installed
# - builder         = builds the KSML Maven project with graal-builder
# - ksml-graal      = base stage + GraalVM/Python copied from the graal-builder stage, creates a venv for the ksml user
# - ksml            = ksml-graal image plus the KSML Runner JAR files and required libraries from builder stage
# - ksml-datagen    = ksml-graal image plus the KSML Data Generator JAR files and required libraries from builder stage


# Step 1: Create the common base image with the ksml user and group and the required packages
FROM registry.access.redhat.com/ubi8/ubi-minimal:8.9-1029 AS base
ENV LANG=en_US.UTF-8

# Environment variable for Connect Build and Runtime
ENV PATH=/opt/graal/bin:$PATH \
	KSML_HOME="/home/ksml" \
    	KSML_INSTALL="/opt/ksml"

RUN set -eux \
        && microdnf install -y procps curl tar unzip gzip zlib openssl-devel gcc gcc-c++ make patch glibc-langpack-en libxcrypt shadow-utils \
	&& groupadd --gid 1024 ksml \
        && useradd -g ksml -u 1024 -d "$KSML_HOME" -ms /bin/sh -f -1 ksml \
        && chown -R ksml:0 "$KSML_HOME" /opt                               \
        && chmod -R g=u /home /opt


# Step 2: Download Graal and Maven into the base image
FROM base AS graal-builder
ARG TARGETARCH
ARG GRAALVM_JDK_VERSION=21.0.1
ARG MAVEN_VERSION=3.9.5

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
    && mkdir -p /opt/maven \
    && MVN_PKG=https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
    && curl --fail --silent --location --retry 3 ${MVN_PKG} | gunzip | tar x -C /opt/maven --strip-components=1 \
#   && GRAALVM_PKG=https://github.com/graalvm/graalvm-ce-builds/releases/download/jdk-21.0.1/graalvm-community-jdk-21.0.1_linux-x64_bin.tar.gz
    && GRAALVM_PKG=https://github.com/graalvm/graalvm-ce-builds/releases/download/jdk-${GRAALVM_JDK_VERSION}/graalvm-community-jdk-${GRAALVM_JDK_VERSION}_linux-${JAVA_ARCH}_bin.tar.gz \
    && mkdir -p /opt/graal \
    && curl --fail --silent --location --retry 3 ${GRAALVM_PKG} | gunzip | tar x -C /opt/graal --strip-components=1

# Step 3: Build the KSML Project, cache the M2 repository location
FROM graal-builder AS builder
ARG TARGETARCH
ADD . /project_dir
WORKDIR /project_dir
RUN \
  --mount=type=cache,target=/root/.m2/repo/$TARGETARCH,id=mvnRepo_$TARGETARCH \
  /opt/maven/bin/mvn -Dmaven.repo.local="/root/.m2/repo/$TARGETARCH" dependency:go-offline --no-transfer-progress \
    && /opt/maven/bin/mvn -Dmaven.repo.local="/root/.m2/repo/$TARGETARCH" --no-transfer-progress package


# Step 4: Build the basic graalvm image stage
FROM base AS ksml-graal
LABEL io.axual.ksml.authors="maintainer@axual.io"
ENV JAVA_HOME=/opt/graalvm
COPY --chown=ksml:0 --from=graal-builder /opt/graal/ /opt/graal/

WORKDIR /home/ksml
USER ksml
#There is no more GraalPy command here and no venv
#RUN graalpy -m venv graalenv && \
#    echo "source $HOME/graalenv/bin/activate" >> ~/.bashrc


# Step 5: Create the KSML Runner image
FROM ksml-graal AS ksml
COPY --chown=ksml:0 --from=builder /project_dir/ksml-runner/target/libs/ /opt/ksml/libs/
COPY --chown=ksml:0 --from=builder /project_dir/graalpy-module-collection/target/modules/ /opt/ksml/modules/
COPY --chown=ksml:0 --from=builder /project_dir/ksml-runner/target/ksml-runner*.jar /opt/ksml/ksml.jar

ENTRYPOINT ["java", "--upgrade-module-path", "/opt/ksml/modules", "-jar", "/opt/ksml/ksml.jar"]

# Step 6: Create the KSML Data Generator image
FROM ksml AS ksml-datagen
