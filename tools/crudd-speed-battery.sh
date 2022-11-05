#!/bin/bash

set -euo pipefail

REGION_SIZE_MIBS=4096
REGION_SIZE_BYTES=$(( REGION_SIZE_MIBS * 1024 * 1024 ))
BLOCK_SIZE=512
REGION_SIZE_BLOCKS=$(( REGION_SIZE_BYTES / BLOCK_SIZE ))
# 128 MiB
EXTENT_SIZE_BLOCKS=$(( 128 * 1024 * 1024 / BLOCK_SIZE ))
EXTENT_COUNT=$(( REGION_SIZE_BLOCKS / EXTENT_SIZE_BLOCKS ))

MAX_ITERATION_DURATION=$((2 * 60))

BENCHMARK_RESULTS_TMPFILE=/tmp/crudd-benchmark-num-bytes-processed.txt

DOWNSTAIRS_1=127.0.0.1:8810
DOWNSTAIRS_2=127.0.0.1:8820
DOWNSTAIRS_3=127.0.0.1:8830

print_err() {
  printf '%s\n' "$1" 1>&2
}

# $1 = mode (read or write)
# $2 = request size in blocks
# $3 = pipeline length
# runs for a maximum time specified by the use of timeout in the function
# prints the average throughput in bytes/second to stdout.
crudd_benchmark() {
  local mode="$1"
  local req_size="$2"
  local pipeline_len="$3"

  local start_time="$(date '+%s')"

  # SIGUSR1 is handled specially by crudd in bechmark mode
  # In benchmark mode, crudd prints how many bytes it wrote to
  # the file passed as an argument to --benchmarking-mode. the timeout
  # ensures crudd never runs _longer_ than the provided duration, but it could
  # run shorter if we're fast and the region is small. So we take bytes written,
  # divided by the time, to get our actual speed. This is all a little bit jank,
  # admittedly.
  timeout --signal=USR1 "$MAX_ITERATION_DURATION" \
    "$BINDIR/crudd" -t $DOWNSTAIRS_1 -t $DOWNSTAIRS_2 -t $DOWNSTAIRS_3 \
      -n $REGION_SIZE_BYTES -i "$req_size" -p "$pipeline_len" \
      --benchmarking-mode "$BENCHMARK_RESULTS_TMPFILE" \
      "$mode" \
      1>&2

  local end_time="$(date +'%s')"
  local bytes_processed="$(cat "$BENCHMARK_RESULTS_TMPFILE")"

  # We lose some precision here, but with speeds in the KiB/s or MiB/s, the
  # rounding loss is not much concern
  printf '%s' $((bytes_processed / (end_time - start_time) ))
}

# create and start downstairs with dsc
create_and_start_downstairs() {
  if [[ -n ${DSC_PID-} ]]; then
    print_err "Error: DSC is already started, but someone tried to start it a second time. Current PID is allegedly $DSC_PID"
    return 1
  fi
  "$BINDIR/dsc" start --cleanup \
    --block-size $BLOCK_SIZE \
    --create \
    --extent-size $EXTENT_SIZE_BLOCKS \
    --extent-count $EXTENT_COUNT \
  1>&2 &
  DSC_PID=$!
  sleep 3
  if ! pgrep -P $DSC_PID > /dev/null; then
    print_err "Failed to start dsc"
    return 1
  fi
  print_err "Started DSC $DSC_PID"
}

# shut down downstairs
stop_downstairs() {
  if ps -p $DSC_PID > /dev/null; then
    print_err "Shutdown $DSC_PID"
    "$BINDIR/dsc" cmd shutdown 1>&2
    print_err "Wait on pid $DSC_PID"
    wait $DSC_PID
    print_err "$DSC_PID dead"
  else
    print_err "tried to stop downstairs, but there is no downstairs"
    return 1
  fi
  DSC_PID=""
}


# - create a region, start up downstairs
# - test read/write before filling the region with data, and read/write after
#   it has data
# - shut down the downstairs
# - print speed results to stdout tab-separated
#   - read speed on an uninitialized region
#   - write speed on an uninitialized region
#   - read speed on a zero-intitialized region
#   - write speed on a zero-initialized region
# $1 = request size
# $2 = pipeline length
perform_benchmarks_with_parameters() {
  # start up downstairs with a fresh region
  create_and_start_downstairs

  local req_size="$1"
  local pipeline_len="$2"

  # test read speed before writing any data
  local read_speed_uninit=$(crudd_benchmark read "$req_size" "$pipeline_len")

  # test write speed before writing any data (first write)
  local write_speed_uninit=$(crudd_benchmark write "$req_size" "$pipeline_len")

  # make sure the entire region is written
  # we use a close to optimal write pattern to make this quick regardless of
  # the benchmark we're running, since we're not timing it out
  "$BINDIR/crudd" -t $DOWNSTAIRS_1 -t $DOWNSTAIRS_2 -t $DOWNSTAIRS_3 \
    -n $REGION_SIZE_BYTES -i 32768 -p 4 \
    write \
    < /dev/zero 1>&2


  # test read speed after writing data
  local read_speed_init=$(crudd_benchmark read "$req_size" "$pipeline_len")

  # test write speed after writing data
  local write_speed_init=$(crudd_benchmark write "$req_size" "$pipeline_len")

  # shut it down
  stop_downstairs

  # print tab separated data as output
  printf '%s\t%s\t%s\t%s' "$read_speed_uninit" "$write_speed_uninit" "$read_speed_init" "$write_speed_init"
}

main() {
  # the maximum amount of time for the full test suite is
  # number of req_size elems * number of pipeline_len elems * 4 * MAX_ITERATION_DURATION
  # 5 * 4 * 4 * 2 = 160 minutes max
  # shellcheck disable=SC1004
  for req_size in 1 16 256 4096 32768; do
    for pipeline_len in 2 4 6 8; do

      printf '%s\t%s\t%s\n' \
        $req_size \
        $pipeline_len \
        "$(perform_benchmarks_with_parameters $req_size $pipeline_len)"
    done
  done \
  | awk '
    {
      request_size = $1;
      pipeline_length = $2;
      read_speed_uninit = $3;
      write_speed_uninit = $4;
      read_speed_init = $5;
      write_speed_init = $6;

      # json time
      print ( \
        "{ " \
        "\"request_size\": "  request_size ", " \
        "\"pipeline_length\": "  pipeline_length ", " \
        "\"read_speed_uninit\": "  read_speed_uninit ", " \
        "\"write_speed_uninit\": "  write_speed_uninit ", " \
        "\"read_speed_init\": "  read_speed_init ", " \
        "\"write_speed_init\": "  write_speed_init \
        " }" \
      );

    }
  ' \
  | tee /tmp/crudd-speed-battery-results.json
}

main