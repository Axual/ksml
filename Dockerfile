# Dockerfile for Axual Connect
# Local Build (ARM64/Apple Mx) docker buildx build --platform=linux/arm64 -t axual/ksml:local --load --target ksml .
# Local Build (AMD65) docker buildx build --platform=linux/amd64 -t axual/ksml:local --load --target ksml .
# Targets
# - base            = UBI image with ksml user and build packages from microdnf
# - graal           = base stage plus GraalVM installed
# - maven           = graal plus maven builds the KSML Maven project
# - ksml            = base plus GraalVM copied from graal and project jars copied from maven

# Step 1: Create the common base image with the ksml user and group and the required packages
FROM registry.access.redhat.com/ubi9/ubi-minimal:9.5 AS base
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
ARG GRAALVM_JDK_VERSION=21.0.2
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


# Step 3: Download and install on top of the Graal image from Step 2. Package KSML project
FROM --platform=$BUILDPLATFORM graal AS maven
ARG MAVEN_VERSION=3.9.10
ARG TARGETARCH
ARG BUILDARCH
RUN set -eux \
    && mkdir -p /opt/maven \
    && MVN_PKG=https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
    && curl --fail --silent --location --retry 3 ${MVN_PKG} | gunzip | tar x -C /opt/maven --strip-components=1

COPY . /project_dir
WORKDIR /project_dir

RUN \
  --mount=type=cache,target=/root/.m2/repo,id=mvnRepo_common \
  /opt/maven/bin/mvn -Dmaven.repo.local="/root/.m2/repo" dependency:go-offline --no-transfer-progress

RUN \
  --mount=type=cache,target=/root/.m2/repo,id=mvnRepo_common \
  if [[ "${TARGETARCH}" = "${BUILDARCH}" ]]; then MVN_ARGS="-DskipTests=false"; echo "Run Tests"; else MVN_ARGS="-DskipTests=true";  echo "Skip Tests for cross compile"; fi \
  && /opt/maven/bin/mvn -Dmaven.repo.local="/root/.m2/repo" -X package $MVN_ARGS


# Step 4: Use the base image and copy GraalVM from Step 2 and the packaged KSML from Step 3
FROM base AS ksml
LABEL io.axual.ksml.authors="maintainer@axual.io"
ENV JAVA_HOME=/opt/graalvm
COPY --chown=ksml:0 --from=graal /opt/graal/ /opt/graal/

WORKDIR /home/ksml
USER 1024
#There is no more GraalPy command here and no venv
#RUN graalpy -m venv graalenv && \
#    echo "source $HOME/graalenv/bin/activate" >> ~/.bashrc

COPY --chown=ksml:0 --from=maven /project_dir/ksml-runner/NOTICE.txt /licenses/THIRD-PARTY-LICENSES.txt
COPY --chown=ksml:0 --from=maven /project_dir/LICENSE.txt /licenses/LICENSE.txt
COPY --chown=ksml:0 --from=maven /project_dir/ksml-runner/target/libs/ /opt/ksml/libs/
COPY --chown=ksml:0 --from=maven /project_dir/ksml-runner/target/ksml-runner*.jar /opt/ksml/ksml.jar

ENTRYPOINT ["java", "-jar", "/opt/ksml/ksml.jar"]
