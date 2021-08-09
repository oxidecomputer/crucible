

This directory contains a stress test for Crucible called Hammer, which does
the following:

1. randomly choose a legal offset and size (not writing past the end of the
   region)
1. fill a buffer of that size with random data
1. write the buffer to that offset
1. read from that offset
1. compare the two buffers, and bail if they're not correct.

This tool is written in Rust. There's also a hammer.c which does the same but
uses `/dev/nbd0` instead of sending work directly to the guest (through the
pseudo file).

    gcc -o hammer hammer.c
    sudo ./hammer

To get crucible-nbd-server running, please follow [this
README](../../nbd_server/src/README.md).

