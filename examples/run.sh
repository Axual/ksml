#!/usr/bin/env bash

# Find out the base directory of this script
# Copied from http://stackoverflow.com/questions/59895/can-a-bash-script-tell-what-directory-its-stored-in
SOURCE="${BASH_SOURCE[0]}"
# resolve $SOURCE until the file is no longer a symlink
while [ -h "$SOURCE" ]; do
    DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
    SOURCE="$(readlink "$SOURCE")"
    # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
    [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
done
BASEDIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

# Give kafka-setup some time to create all the topics
echo "This script assumes the example docker-compose has started and the local KSML image was created"
sleep 2
docker run --name ksml-example --rm -ti -v "${BASEDIR}":/ksml -w /ksml --network ksml_example axual/ksml:latest
