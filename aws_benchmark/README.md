# Crucible AWS benchmark test.
These scripts and tools will:
 * Create instances in AWS
 * Run performance tests
 * Remove the instances.

In addition to other software, you should have configured access to AWS using the `aws` command line program and configured your credentials.  You can find more info on how to install the `aws` command here: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html

## How to run the tests
The main program to drive the test is driver.sh. It will verify existance of some needed programs and serves as the outer loop for the steps listed above.

Specifically, driver.sh will benchmark Crucible by doing the following:

- use terraform to create three downstairs and one upstairs
- use ansible to (among other things):
    - ship this repo to each instance,
    - compile crucible,
    - create a 2G region on the downstairs,
    - start the downstairs as a service
- run bench.sh on the upstairs, saving the timing output to results.txt
- clean up the resources

Both helios and ubuntu are supported as host operating systems, and AWS region
is a parameter:

```
./driver.sh helios us-east-1
```
or
```
./driver.sh ubuntu ca-central-1
```

## Results
Results are output into the file `results.txt`, which is `cat`ed at the end of driver.sh.

Any error results in cleanup being performed. Add an `echo` to the terraform
apply -destroy command in cleanup() if you want to debug further, but make sure
to clean up your AWS resources!

## Customization
Edit bench.sh to modify which program is run as part of the benchmark.

To individually run steps:

* ./bring_up_resources.sh will bring up AWS resources and run ansible to
  install everything that's required
* ./run_benchmark.sh will run a warm up plus benchmarking step
* ./cleanup.sh will deprovision all AWS resources

