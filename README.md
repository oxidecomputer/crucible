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

```
$ mkdir -p var/3801
$ cargo run -q -p crucible-downstairs -- -p 3801 -d var/3801
raw options: Opt { address: 0.0.0.0, port: 3801, data: "var/3801" }
listening on 0.0.0.0:3801
...
```

```
$ mkdir -p var/3802
$ cargo run -q -p crucible-downstairs -- -p 3802 -d var/3802
raw options: Opt { address: 0.0.0.0, port: 3802, data: "var/3802" }
listening on 0.0.0.0:3802
...
```

```
$ mkdir -p var/3803
$ cargo run -q -p crucible-downstairs -- -p 3803 -d var/3803
raw options: Opt { address: 0.0.0.0, port: 3803, data: "var/3803" }
listening on 0.0.0.0:3803
...
```

Then, connect to them with a single Upstairs process:

```
$ cargo run -q -p crucible-upstairs -- \
    -t 127.0.0.1:3801 -t 127.0.0.1:3802 -t 127.0.0.1:3803
raw options: Opt { target: [127.0.0.1:3801, 127.0.0.1:3802, 127.0.0.1:3803] }
target list: [
    127.0.0.1:3801,
    127.0.0.1:3802,
    127.0.0.1:3803,
]
looper start for 127.0.0.1:3802
looper start for 127.0.0.1:3801
looper start for 127.0.0.1:3803
connecting to 127.0.0.1:3802
connecting to 127.0.0.1:3803
connecting to 127.0.0.1:3801
ok, connected to 127.0.0.1:3803
ok, connected to 127.0.0.1:3802
ok, connected to 127.0.0.1:3801
127.0.0.1:3803: version 1
127.0.0.1:3801: version 1
127.0.0.1:3803 #### CONNECTED ########
127.0.0.1:3802: version 1
127.0.0.1:3801 #### CONNECTED ########
~ ~ ~ 127.0.0.1:3801: request from main thread! ~ ~ ~
127.0.0.1:3802 #### CONNECTED ########
~ ~ ~ 127.0.0.1:3803: request from main thread! ~ ~ ~
~ ~ ~ 127.0.0.1:3801: request from main thread! ~ ~ ~
~ ~ ~ 127.0.0.1:3802: request from main thread! ~ ~ ~
127.0.0.1:3801 --recv--> Imok
127.0.0.1:3803 --recv--> Imok
127.0.0.1:3802 --recv--> Imok
127.0.0.1:3803 --recv--> Imok
127.0.0.1:3802 --recv--> Imok
127.0.0.1:3801 --recv--> Imok
...
```

On the console of each Downstairs, you will see a connection; e.g.,

```
...
raw options: Opt { address: 0.0.0.0, port: 3801, data: "var/3801" }
listening on 0.0.0.0:3801
connection 1 from 127.0.0.1:59893
1: version 1
...
```

That's all for now!
