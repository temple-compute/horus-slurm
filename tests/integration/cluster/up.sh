#!/usr/bin/env bash
# Start the itest cluster and wait until both nodes are idle and
# accounting answers. Needs ITEST_DIR; ITEST_PROJECT names the compose
# project.
set -euo pipefail

: "${ITEST_DIR:?set ITEST_DIR}"
export ITEST_DIR
project=${ITEST_PROJECT:-horus-slurm-itest}
compose=(docker compose -p "$project" -f "$(dirname "$0")/compose.yaml")
uid=$(id -u)

case "$ITEST_DIR" in
    / | "$HOME" | "$HOME"/) echo "ITEST_DIR=$ITEST_DIR is too broad" >&2; exit 1 ;;
esac
# Before compose, or docker creates it owned by root. The pilot runs
# launchers found below it, so nobody else may own or swap it.
mkdir -p "$ITEST_DIR"
if [ ! -O "$ITEST_DIR" ] || [ -L "$ITEST_DIR" ]; then
    echo "ITEST_DIR=$ITEST_DIR must be a directory we own" >&2
    exit 1
fi
chmod 700 "$ITEST_DIR"
"${compose[@]}" up -d --no-build

# Jobs run as the host uid so files on the bind mount stay ours.
# Containers lose the user on recreate, so check every time.
for svc in slurmctld c1 c2; do
    "${compose[@]}" exec -T "$svc" bash -c \
        'getent passwd "$1" >/dev/null || useradd -u "$1" -M -d "$2" itest' \
        _ "$uid" "$ITEST_DIR"
done

while (( SECONDS < 120 )); do
    idle=$("${compose[@]}" exec -T slurmctld sinfo -h -N -o %T 2>/dev/null |
        grep -c '^idle$' || true)
    cluster=$("${compose[@]}" exec -T slurmctld sacctmgr -n list cluster \
        format=cluster 2>/dev/null || true)
    if [ "$idle" = 2 ] && grep -qw itest <<<"$cluster"; then
        echo "cluster up"
        exit 0
    fi
    sleep 1
done

"${compose[@]}" logs
echo "cluster not ready after 120 s" >&2
exit 1
