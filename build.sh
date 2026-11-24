#!/bin/bash
set -euo pipefail

cd -- "$(dirname -- "${BASH_SOURCE[0]}")"

mode="${1:---push}"
if [[ $# -gt 1 || ( "$mode" != --push && "$mode" != --load ) ]]; then
    printf 'Usage: %s [--push|--load]\n' "$0" >&2
    exit 2
fi

image_name="${IMAGE_NAME:-luql11/r86s}"
version="$(mvn -B -q -Dstyle.color=never -Djansi.mode=strip \
    help:evaluate -Dexpression=project.version -DforceStdout | tr -d '\r')"
if [[ ! "$version" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]]; then
    printf 'Maven project.version is not a valid Docker tag: %s\n' "$version" >&2
    exit 1
fi

if [[ "$mode" == --push ]]; then
    : "${DOCKER_USERNAME:?Set DOCKER_USERNAME to your Docker Hub username}"
    : "${DOCKER_PASSWORD:?Set DOCKER_PASSWORD to your Docker Hub token}"
    printf '%s\n' "$DOCKER_PASSWORD" | docker login --username "$DOCKER_USERNAME" --password-stdin
fi

image_tag="${image_name}:${version}"
printf 'Building %s for linux/amd64 (%s)\n' "$image_tag" "$mode"
docker buildx build --platform linux/amd64 --progress=plain --tag "$image_tag" "$mode" .
