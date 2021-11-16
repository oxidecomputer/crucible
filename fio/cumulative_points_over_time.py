#!/usr/bin/env python3

import sys
import math

# time (msec), value, data direction, block size (bytes), offset (bytes)

for fi in sys.argv[1:]:
    with open(fi) as fp:
        text = fp.read()

    with open(fi + ".cumulative", "w") as fp:
        # at time 0, there are 0 points
        fp.write("0,0\n")

        points = 1
        for line in text.split("\n"):
            if not line:
                continue

            time, value, direction, bs, offset = \
                [int(x.strip()) for x in line.strip().split(",")]

            fp.write("{},{}\n".format(time, points))
            points += 1

