#!/bin/bash

# Script to build KSML Docker image locally for testing
# This script prepares build artifacts and builds the Docker image using the main Dockerfile

set -e  # Exit on any error

mvn clean package -DskipITs=true -P '!sonarqube'

# Prepare build artifacts
echo "  - Creating build-output/ directory"
# Start from a clean directory. Leftovers from an earlier build put several versions of the same
# library in the image, which breaks class loading in ways that are hard to spot.
rm -rf build-output
echo "  - Copying ksml-runner JAR, ksml-test-runner JAR, libraries, and license files"

mkdir -p build-output
cp ksml-runner/target/ksml-runner*.jar build-output/
cp ksml-test-runner/target/ksml-test-runner*.jar build-output/
cp -r ksml-runner/target/libs build-output/
# Copy test-runner libs on top, so both manifests find the JARs they reference
cp ksml-test-runner/target/libs/*.jar build-output/libs/
cp ksml-runner/NOTICE.txt build-output/
cp LICENSE.txt build-output/
# Download graalvm tarfiles. Version comes from .graalvm-jdk-version; override it with
# GRAALVM_JDK_VERSION=... to try another one.
scripts/graalvm-tarballs.sh download

# Create builder if it doesn't exist
if ! docker buildx ls --format {{.Name}} | grep -E "^ksml$"; then
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
