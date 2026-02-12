#!/bin/bash

# Script to build KSML Docker image locally for testing
# This script prepares build artifacts and builds the Docker image using the main Dockerfile

set -e  # Exit on any error

mvn package -DskipITs=true

# Prepare build artifacts
echo "  - Creating build-output/ directory"
echo "  - Copying ksml-runner JAR, libraries, and license files"

mkdir -p build-output
cp ksml-runner/target/ksml-runner*.jar build-output/
cp -r ksml-runner/target/libs build-output/
cp ksml-runner/NOTICE.txt build-output/
cp LICENSE.txt build-output/

# Download graalvm tarfiles
if [ ! -f graalvm-amd64.tar.gz ]; then
  echo "Downloading GraalVM for AMD64"
  wget https://github.com/graalvm/graalvm-ce-builds/releases/download/jdk-${GRAALVM_JDK_VERSION}/graalvm-community-jdk-${GRAALVM_JDK_VERSION}_linux-x64_bin.tar.gz -O graalvm-amd64.tar.gz
fi
if [ ! -f graalvm-arm64.tar.gz ]; then
  echo "Downloading GraalVM for ARM64"
  wget https://github.com/graalvm/graalvm-ce-builds/releases/download/jdk-${GRAALVM_JDK_VERSION}/graalvm-community-jdk-${GRAALVM_JDK_VERSION}_linux-aarch64_bin.tar.gz -O graalvm-arm64.tar.gz
fi

# Create builder if it doesn't exist
if ! docker docker buildx ls --format {{.Name}} | grep -E "^ksml$"; then
    echo "Creating Docker buildx builder 'ksml'..."
    docker buildx create --name ksml --use
else
    echo "Using existing Docker buildx builder 'ksml'..."
    docker buildx use ksml
fi

docker buildx build \
  --load \
  -t axual/ksml:local \
  --target ksml \
  -f Dockerfile \
  .