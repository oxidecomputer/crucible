#!/bin/bash

# this is a simple test script that takes 3 targets, writes some data,
# and reads it back. I don't know how to integrate with our CI yet so
# for now this is what you're getting so I can go work on something
# else for a bit

if [ $# -lt 3 ]; then
  echo "Usage: $0 <target1> <target2> <target3>"
  echo "For example, $0 127.0.0.1:3010 127.0.0.1:3020 127.0.0.1:3030"
  exit 1
fi

# generate some temp data
tmpfile="$(mktemp)"
tmpout="$(mktemp)"

cleanup() {
  rm "$tmpfile"
  rm "$tmpout"
}

die() {
  echo "$@"
  echo "Not cleaning up $tmpfile or $tmpout"
  exit 1
}

#512 megs of data
tmpbytes=$(( 512 * 1024 * 1024))
dd if=/dev/urandom of="$tmpfile" bs=4M count=$((tmpbytes / ( 4 * 1024 * 1024 ) ))

# test aligned write/read
cat "$tmpfile" | cargo run --release -- -g 1 -b 0 -n $tmpbytes -t "$1" -t "$2" -t "$3" write
cargo run --release -- -g 2 -b 0 -n "$tmpbytes" -t "$1" -t "$2" -t "$3" 3>"$tmpout" read

if diff -q "$tmpfile" "$tmpout"; then
  echo "Success: aligned read/write"
else
  die "Failure: aligned read/write"
fi


# misaligned write/read
dd if=/dev/urandom of="$tmpfile" bs=4M count=$((tmpbytes / ( 4 * 1024 * 1024 ) ))
cat "$tmpfile" | cargo run --release -- -g 3 -b 29 -n $tmpbytes -t "$1" -t "$2" -t "$3" write
cargo run --release -- -g 4 -b 29 -n "$tmpbytes" -t "$1" -t "$2" -t "$3" 3>"$tmpout" read

if diff -q "$tmpfile" "$tmpout"; then
  echo "Success: misaligned read/write"
else
  die "Failure: misaligned read/write"
fi

# nano-read/write
nanosize=39
dd if=/dev/urandom of="$tmpfile" bs=$nanosize count=1
cat "$tmpfile" | cargo run --release -- -g 5 -b 647 -n $nanosize -t "$1" -t "$2" -t "$3" write
cargo run --release -- -g 6 -b 647 -n $nanosize -t "$1" -t "$2" -t "$3" 3>"$tmpout" read

if diff -q "$tmpfile" "$tmpout"; then
  echo "Success: nano read/write"
else
  die "Failure: nano read/write"
fi

cleanup
exit 0
