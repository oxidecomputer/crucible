#!/usr/bin/env python

import json
import sys


with open(sys.argv[1]) as fp:
    d = json.load(fp)

block_size = int(d["block_size"])
extent_size = int(d["extent_size"]["value"])
extent_count = int(d["extent_count"])

total_bytes = block_size * extent_size * extent_count

with open("crucible.fio", "w") as fp:
    fp.write("""[global]
rw=randwrite
bs={}
size={}
ioengine=libaio
iodepth=32
direct=1
write_bw_log
write_lat_log

[crucible_{}]
filename=/dev/nbd0""".format(block_size, total_bytes, sys.argv[2]))

