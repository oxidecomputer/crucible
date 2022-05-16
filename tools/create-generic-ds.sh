#!/bin/bash
#
# A hack of downstairs create tool

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT" || (echo failed to cd "$ROOT"; exit 1)
export BINDIR=${BINDIR:-$ROOT/target/debug}

cds="$BINDIR/crucible-downstairs"
if [[ ! -f "$cds" ]]; then
    echo "Can't find crucible binary at $cds" >&2
    exit 1
fi

usage () {
    echo "Usage: $0 [de] [-b #] [-c #] [-s #]" >&2
    echo " -b block_size    Block size for the region" >&2
    echo " -c extent_count  Total number of extent files" >&2
    echo " -d               Delete existing var/88* direcories" >&2
    echo " -e               Require encryption on the volume" >&2
    echo " -h               Show this help message" >&2
    echo " -s extent_size   Number of extents per extent file" >&2
}

block_size=512
delete=0
encryption=0
extent_size=100
extent_count=20

while getopts 'b:c:dehs:' opt; do
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
        h)  usage
            exit 0
            ;;
        s)  extent_size=$OPTARG
            echo "Using extent size $extent_size"
            ;;
        *)  echo "Invalid option"
            usage
            exit 1
            ;;
    esac
done

if pgrep -fl -U $(id -u) crucible-downstairs; then
    echo 'Some downstairs already running?' >&2
    exit 1
fi

if [[ $delete -eq 1 ]]; then
    rm -rf var/8810 var/8820 var/8830
else
    if [[ -d var/8810 ]] || [[ -d var/8820 ]] || [[ -d var/8830 ]]; then
        echo " var/88.. directories are already present"
        exit 1
    fi
fi

res=0
for port in 8810 8820 8830; do
    if [[ $encryption -eq 0 ]]; then
        echo "$cds" create -u 12345678-"$port"-"$port"-"$port"-00000000"$port" -d var/"$port" --extent-count "$extent_count" --extent-size "$extent_size" --block-size "$block_size"
        if ! time "$cds" create -u 12345678-"$port"-"$port"-"$port"-00000000"$port" -d var/"$port" --extent-count "$extent_count" --extent-size "$extent_size" --block-size "$block_size"; then
            echo "Failed to create downstairs $port"
            res=1
        fi
    else
        echo "$cds" create -u 12345678-"$port"-"$port"-"$port"-00000000"$port" -d var/"$port" --extent-count "$extent_count" --extent-size "$extent_size"  --block-size "$block_size" --encrypted=true
        if ! time "$cds" create -u 12345678-"$port"-"$port"-"$port"-00000000"$port" -d var/"$port" --extent-count "$extent_count" --extent-size "$extent_size"  --block-size "$block_size" --encrypted=true; then
            echo "Failed to create downstairs $port"
            res=1
        fi
    fi
done
exit $res
