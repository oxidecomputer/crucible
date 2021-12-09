`driver.sh` will benchmark Crucible by doing the following:

- use terraform to create three downstairs and one upstairs
- use ansible to (among other things):
    - ship this repo to each instance,
    - compile crucible,
    - create a 2G region on the downstairs,
    - start the downstairs as a service
- run `bench.sh` on the upstairs, saving the timing output to results.txt
- clean up the resources

Both helios and ubuntu are supported as host operating systems, and AWS region
is a parameter:

    ./driver.sh helios us-east-1

    ./driver.sh ubuntu ca-central-1

Results are output into results.txt, which is `cat`ed at the end of driver.sh.

Any error results in cleanup being performed. Add an `echo` to the terraform
apply -destroy command in cleanup() if you want to debug further, but make sure
to clean up your AWS resources!

