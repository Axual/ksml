#!/bin/bash
#
# Downloads the GraalVM Community JDK that the Docker image runs on.
#
#   graalvm-tarballs.sh download   download both tarballs into this directory
#   graalvm-tarballs.sh check      only check both URLs exist, download nothing
#
# The version comes from .graalvm-jdk-version. It must be the same as <graalvm.version> in
# pom.xml: the GraalVM libraries only work on the GraalVM runtime of the same release.
#
# Keep the output names as they are. The Dockerfile unpacks
# graalvm-${TARGETARCH}.tar.gz, and Docker sets TARGETARCH to amd64 or arm64.

set -eu

mode=${1:-download}
if [ "$mode" != "download" ] && [ "$mode" != "check" ]; then
    echo "Usage: $0 [download|check]" >&2
    exit 2
fi

repo_root=$(cd "$(dirname "$0")/.." && pwd)
version=$(tr -d '[:space:]' < "$repo_root/.graalvm-jdk-version")

if [ -z "$version" ]; then
    echo "No version found in $repo_root/.graalvm-jdk-version" >&2
    exit 1
fi

pom_version=$(sed -n 's:.*<graalvm.version>\(.*\)</graalvm.version>.*:\1:p' "$repo_root/pom.xml" | head -1)
if [ "$version" != "$pom_version" ]; then
    echo "GraalVM runtime ${version} (.graalvm-jdk-version) is not the same as the GraalVM" >&2
    echo "libraries ${pom_version} (<graalvm.version> in pom.xml). Change both to the same version." >&2
    exit 1
fi

# Up to 25.0.2: tag jdk-<version>, file graalvm-community-jdk-<version>_...
# From 25.1: tag graal-<version>, file graalvm-community-jdk-25i<N>-25.0.<rest>_... (25.5.5.0.3 -> 25i5-25.0.5.0.3).
major=${version%%.*}
rest=${version#*.}
minor=${rest%%.*}
if [ "$major" -lt 25 ] || { [ "$major" -eq 25 ] && [ "$minor" -eq 0 ]; }; then
    tag="jdk-${version}"
    file_version="${version}"
else
    tag="graal-${version}"
    file_version="${major}i${minor}-${major}.0.${rest#*.}"
fi

base_url=https://github.com/graalvm/graalvm-ce-builds/releases/download
failed=0

# GraalVM calls these architectures x64 and aarch64, Docker calls them amd64 and
# arm64, so each file is renamed as it is downloaded.
for pair in x64:amd64 aarch64:arm64; do
    graal_arch=${pair%%:*}
    docker_arch=${pair##*:}
    url="${base_url}/${tag}/graalvm-community-jdk-${file_version}_linux-${graal_arch}_bin.tar.gz"
    output="graalvm-${docker_arch}.tar.gz"

    if [ "$mode" = "check" ]; then
        if wget --spider -q "$url"; then
            echo "ok       GraalVM JDK ${version} linux-${graal_arch}"
        else
            echo "MISSING  GraalVM JDK ${version} linux-${graal_arch}"
            echo "         ${url}"
            failed=1
        fi
        continue
    fi

    # -s rather than -f: a failed download leaves an empty file behind, and treating that
    # as finished produces a broken image later on.
    if [ -s "$output" ]; then
        echo "Already downloaded: ${output}"
        continue
    fi

    echo "Downloading GraalVM JDK ${version} for ${docker_arch}"
    if ! wget "$url" -O "$output"; then
        rm -f "$output"
        failed=1
    fi
done

if [ "$failed" -ne 0 ]; then
    cat >&2 <<EOF

GraalVM Community ${version} is not published as a tarball under the URL above, so the
Docker image cannot be built. See https://github.com/graalvm/graalvm-ce-builds/releases
for what exists. Note that Maven Central also has 25.0.x versions after 25.0.2 (25.0.3,
25.0.5.0.3, ...). Those are only libraries: there is no GraalVM Community runtime for them.
EOF
    exit 1
fi
