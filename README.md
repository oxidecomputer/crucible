# Oxide Crucible

A prototype storage service.  There are two components:

* `crucible-downstairs`: will reside with the target disk storage and provide
  access to it via the network for those upstairs
* `crucible-upstairs`: will reside with the machine using the storage,
  making requests across the network to some number of downstairs replicas

As this is under active development, this space is subject to change.
The steps below still work, but may give slightly different output as
more code is written.

To give it a burl, first build the workspace with `cargo build`.  Then run
a set of Downstairs processes:

To create and run a downstairs, use the `-c` flag.  If you have already
created a downstairs region, you should not use the `-c` flag, it will only
work on the first creation.
```
$ cargo run -q -p crucible-downstairs -- -c -p 3801 -d var/3801
raw options: Opt { address: 0.0.0.0, port: 3801, data: "var/3801" }
listening on 0.0.0.0:3801
...
```

```
$ cargo run -q -p crucible-downstairs -- -c -p 3802 -d var/3802
raw options: Opt { address: 0.0.0.0, port: 3802, data: "var/3802" }
listening on 0.0.0.0:3802
...
```

```
$ cargo run -q -p crucible-downstairs -- -c -p 3803 -d var/3803
raw options: Opt { address: 0.0.0.0, port: 3803, data: "var/3803" }
listening on 0.0.0.0:3803
...
```

Then, connect to them by using the crucible client program that will
start the upstairs side of crucible for you, run a write/flush/read,
then exit.

```
$ cargo run -q -p crucible -- -t 127.0.0.1:3803 -t 127.0.0.1:3802 -t 127.0.0.1:3801
raw options: Opt { target: [127.0.0.1:3803, 127.0.0.1:3802, 127.0.0.1:3801] }
runtime is spawned
DTrace probes registered ok
127.0.0.1:3802[1] connecting to 127.0.0.1:3802
127.0.0.1:3803[0] connecting to 127.0.0.1:3803
127.0.0.1:3801[2] connecting to 127.0.0.1:3801
127.0.0.1:3802[1] ok, connected to 127.0.0.1:3802
127.0.0.1:3803[0] ok, connected to 127.0.0.1:3803
127.0.0.1:3801[2] ok, connected to 127.0.0.1:3801
127.0.0.1:3801 Evaluate new downstairs : bs:512 es:100 ec:10 versions: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
Set inital Extent versions to [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
Next flush: 1
Global using: bs:512 es:100 ec:10
127.0.0.1:3803 Evaluate new downstairs : bs:512 es:100 ec:10 versions: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
#### 127.0.0.1:3801 #### CONNECTED ######## 1/3
#### 127.0.0.1:3803 #### CONNECTED ######## 2/3
127.0.0.1:3802 Evaluate new downstairs : bs:512 es:100 ec:10 versions: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
#### 127.0.0.1:3802 #### CONNECTED ######## 3/3
send a write
send a flush
nwo: [(0, 99, 512), (1, 0, 512)] from offset:50688 data: 0x7fb053008200 len:1024
send a read
Tests done, wait
WRITE: gw_id:1 ds_ids:{1001: 512, 1000: 512}
FLUSH: gw_id:2 ds_ids:{1002: 0}
nwo: [(0, 99, 512), (1, 0, 512)] from offset:50688 data len:1024
READ:  gw_id:3 ds_ids:{1003: 512, 1004: 512}
[1] Write ds_id:1000 eid:0 bo:99
[0] Write ds_id:1000 eid:0 bo:99
[1] Write ds_id:1001 eid:1 bo:0
[0] Write ds_id:1001 eid:1 bo:0
Flush ds_id:1002 dep:2 flush_number:1
Flush ds_id:1002 dep:2 flush_number:1
Read  ds_id:1003 eid:0 bo:99 blocks:1
[2] Write ds_id:1000 eid:0 bo:99
Read  ds_id:1003 eid:0 bo:99 blocks:1
Read  ds_id:1004 eid:1 bo:0 blocks:1
Read  ds_id:1004 eid:1 bo:0 blocks:1
[2] Write ds_id:1001 eid:1 bo:0
Flush ds_id:1002 dep:2 flush_number:1
Read  ds_id:1003 eid:0 bo:99 blocks:1
Read  ds_id:1004 eid:1 bo:0 blocks:1
RETIRE:  ds_id 1000 from gw_id:1
RETIRE:  ds_id 1001 from gw_id:1
Save data for ds_id:1003
Save data for ds_id:1004
RETIRE:  ds_id 1002 from gw_id:2
RETIRE:  ds_id 1003 from gw_id:3
gw_id:3 Save read buffer for 1003
RETIRE:  ds_id 1004 from gw_id:3
gw_id:3 Save read buffer for 1004
Final data copy [1003, 1004]
all Tests done
```

On the console of each Downstairs, you will see a connection; e.g.,

```
...
raw options: Opt { address: 0.0.0.0, port: 3801, data: "var/3801", create: true }
Create new extent directory
created new region file "var/3801/region.json"
Current flush_numbers: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
Startup Extent values: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
listening on 0.0.0.0:3801
connection from 127.0.0.1:65030  connections count:1
Current flush_numbers: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
Write       rn:1000 eid:0 dep:[] bo:99
Write       rn:1001 eid:1 dep:[] bo:0
flush       rn:1002 dep:[1000, 1001] fln:1
Read        rn:1003 eid:0 bo:99
Read        rn:1004 eid:1 bo:0
OK: connection(1): all done
```

That's all for now!

# Tracing #

Run a Jaeger container in order to collect and visualize traces:

    $ docker run --rm -d --name jaeger \
      -e COLLECTOR_ZIPKIN_HOST_PORT=:9411 \
      -p 5775:5775/udp \
      -p 6831:6831/udp \
      -p 6832:6832/udp \
      -p 5778:5778 \
      -p 16686:16686 \
      -p 14268:14268 \
      -p 14250:14250 \
      -p 9411:9411 \
      jaegertracing/all-in-one:1.24

Pass an option to crucible-downstairs to send traces to Jaeger:

    $ cargo run -q -p crucible-downstairs -- -c -p 3803 -d var/3803 --trace-endpoint localhost:6831

Then, go to `http://localhost:16686` to see the Jaeger UI.

