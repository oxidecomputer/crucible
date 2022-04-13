#!/bin/bash
#
# A hack of downstairs create tool

ROOT=$(cd "$(dirname "$0")/.." && pwd)

cd "$ROOT" || (echo failed to cd "$ROOT"; exit 1)

if pgrep -fl crucible-downstairs; then
    echo 'Some downstairs already running?' >&2
    exit 1
fi

if ! cargo build; then
    echo "Initial Build failed, no tests ran"
    exit 1
fi

block_size=512
delete=0
encryption=0
extent_size=100
extent_count=20
while getopts 'b:c:des:' opt; do
    case "$opt" in
        b)  block_size=$OPTARG
            echo "Using block size $block_size"
            ;;
        c)  extent_count=$OPTARG
            echo "Using extent count $extent_count"
            ;;
        d)  delete=1
            echo "delete and re-create any region dirs"
            ;;
        e)  encryption=1
            echo "enable encryption"
            ;;
        s)  extent_size=$OPTARG
            echo "Using extent size $extent_size"
            ;;
        *)  echo "Usage: $0 [de] [-c extent_count] [-s extent_size]" >&2
            exit 1
            ;;
    esac
done

if [[ $delete -eq 1 ]]; then
    rm -rf var/8810 var/8820 var/8830
else
    if [[ -d var/8810 ]] || [[ -d var/8820 ]] || [[ -d var/8830 ]]; then
        echo " var/88.. directories are already present"
        exit 1
    fi
fi

cds="./target/release/crucible-downstairs"
if [[ ! -f ${cds} ]]; then
    cds="./target/debug/crucible-downstairs"
    if [[ ! -f ${cds} ]]; then
        echo "Can't find crucible binary at $cds"
        exit 1
    fi
fi

res=0
for port in 8810 8820 8830; do
    if [[ $encryption -eq 0 ]]; then
        if ! cargo run -q -p crucible-downstairs -- create -u 12345678-"$port"-"$port"-"$port"-00000000"$port" -d var/"$port" --extent-count "$extent_count" --extent-size "$extent_size" --block-size "$block_size"; then
            echo "Failed to create downstairs $port"
            res=1
        fi
    else
        if ! cargo run -q -p crucible-downstairs -- create -u 12345678-"$port"-"$port"-"$port"-00000000"$port" -d var/"$port" --extent-count "$extent_count" --extent-size "$extent_size"  --block-size "$block_size" --encrypted=true; then
            echo "Failed to create downstairs $port"
            res=1
        fi

    fi
done
exit $res
