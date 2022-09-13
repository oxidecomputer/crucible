# crudd

It's `dd`, for crucible! Now with 80% less draconic command arguments!

You can use this to read data from some downstairses, or write data to them. `crudd`
goes through Upstairs, so it supports things like encryption and replication.

Some things you could do with crudd:

- test read/write performance of your downstairs or upstairs implementations.
- image a region with some data.
- grab a copy of a region to check if it looks like what you expect it to look like.
- perform small nano-writes in a region.
- make sure your ISO header is where you expect it to be.

Some things you can't do with crudd (yet):

- Literally anything involving the Volumes feature

See the help for the most up to date options.

There is one drawback: in exchange for better command arguments, you get a
hard-requirement on shell IO stream redirection for reads! It's the law of
equivalent exchange. To obtain, something of equal value must be lost.

Anyway, since Upstairs writes a lot of stuff to STDOUT and I don't know how
to turn it off, I write output to FD3 for the `read` subcommand. That means
you need to run it a bit like this:

```
# Redirect crudd's FD3 to your STDOUT, and crudd's STDOUT to your STDERR
# Then you can pipe it around like normal
crudd -t 127.0.0.1:3010 -t 127.0.0.1:3020 -t 127.0.0.1:3030 read 3>&1 1>&2 | wc -c

# If you just want to write to a file, it's simpler
crudd -t 127.0.0.1:3010 -t 127.0.0.1:3020 -t 127.0.0.1:3030 read 3>outputfile.img
```


`write` is unaffected, so writes are more normal:

```
cat inputfile.img | crudd -t 127.0.0.1:3010 -t 127.0.0.1:3020 -t 127.0.0.1:3030 write
```