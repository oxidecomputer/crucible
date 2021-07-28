#!/usr/bin/env python3

import sys
import math


# bandwidth: value is KiB/sec
# time (msec), value, data direction, block size (bytes), offset (bytes)

for fi in sys.argv[1:]:
    bins = 500
    min_v = 0
    max_v = 0

    with open(fi) as fp:
        text = fp.read()

    for line in text.split("\n"):
        if not line:
            continue

        time, value, direction, bs, offset = \
            [int(x.strip()) for x in line.strip().split(",")]

        if value > max_v:
            max_v = value

    binwidth = (max_v - min_v) / bins

    with open(fi) as fp:
        text = fp.read()

    packed = {}

    for i in range(0, bins):
        packed[i] = 0

    for line in text.split("\n"):
        if not line:
            continue

        time, value, direction, bs, offset = \
            [int(x.strip()) for x in line.strip().split(",")]

        if value < min_v or value >= max_v:
            continue

        histbin = math.floor((value-min_v) / binwidth)
        packed[histbin] += 1

    with open(fi + ".hist", "w") as fp:
        for i in range(0, bins):
            fp.write("{},{}\n".format(i * binwidth, packed[i]))

