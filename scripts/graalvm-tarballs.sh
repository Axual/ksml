#!/bin/bash
#
# Downloads the GraalVM Community JDK that the Docker image runs on.
#
#   graalvm-tarballs.sh download   download both tarballs into this directory
#   graalvm-tarballs.sh check      only check both URLs exist, download nothing
#
# The version comes from .graalvm-jdk-version. Set GRAALVM_JDK_VERSION to try a
# different one without editing that file.
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
version=${GRAALVM_JDK_VERSION:-$(tr -d '[:space:]' < "$repo_root/.graalvm-jdk-version")}

if [ -z "$version" ]; then
    echo "No version found in $repo_root/.graalvm-jdk-version" >&2
    exit 1
fi

base_url=https://github.com/graalvm/graalvm-ce-builds/releases/download
failed=0

# GraalVM calls these architectures x64 and aarch64, Docker calls them amd64 and
# arm64, so each file is renamed as it is downloaded.
for pair in x64:amd64 aarch64:arm64; do
    graal_arch=${pair%%:*}
    docker_arch=${pair##*:}
    url="${base_url}/jdk-${version}/graalvm-community-jdk-${version}_linux-${graal_arch}_bin.tar.gz"
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

GraalVM Community JDK ${version} is not published as a tarball, so the Docker image
cannot be built. See https://github.com/graalvm/graalvm-ce-builds/releases for what
exists. Releases after 25.0.2 use a "graal-" tag and a "25iN-" file name, which the URL
above cannot produce, so those need a change here and not just a new version number.

The <graalvm.version> in pom.xml is a separate thing. It selects the org.graalvm.* Maven
artifacts, which are published independently, so it may legitimately differ from this.
EOF
    exit 1
fi
