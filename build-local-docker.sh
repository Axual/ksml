#!/bin/bash

# Script to build KSML Docker image locally for testing
# This script prepares build artifacts and builds the Docker image using the main Dockerfile

set -e  # Exit on any error

mvn clean verify -DskipITs=false

# Prepare build artifacts
echo "  - Creating build-output/ directory"
echo "  - Copying ksml-runner JAR, libraries, and license files"

mkdir -p build-output
cp ksml-runner/target/ksml-runner*.jar build-output/
cp -r ksml-runner/target/libs build-output/
cp ksml-runner/NOTICE.txt build-output/
cp LICENSE.txt build-output/

# Create builder if it doesn't exist
if ! docker buildx ls | grep -q "^ksml"; then
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