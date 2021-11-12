set terminal png nocrop enhanced size 1920,1080

set datafile separator ','

# time (msec), value, data direction, block size (bytes), offset (bytes)
set xlabel 'msec'
#set logscale y
#set yrange [0:4000]

# Bandwidth log: Value is in KiB/sec
set ylabel 'KiB/sec'

set output 'crucible_bw.png'
plot \
    'crucible_before_bw.1.log' using 1:2 lc rgb"blue" title 'before', \
    'crucible_after_bw.1.log' using 1:2 lc rgb"red" title 'after'

# Latency log: Value is latency in nsecs

unset xrange
#set yrange [0:300000000]
set ylabel 'nsec'
unset logscale y

set output 'crucible_lat.png'

plot \
    'crucible_before_lat.1.log' using 1:2 lc rgb"blue" title 'before', \
    'crucible_after_lat.1.log' using 1:2 lc rgb"red" title 'after'

# cumulative points over time
set ylabel 'cumulative points'
set output 'crucible_cumulative.png'

plot \
    'crucible_before_bw.1.log.cumulative' using 1:2 lc rgb"blue" title 'before', \
    'crucible_after_bw.1.log.cumulative' using 1:2 lc rgb"red" title 'after'

# histograms

set xlabel 'KiB/sec'
set ylabel 'frequency'

#unset yrange

set output 'crucible_bw_histogram.png'
set xlabel 'KiB/sec'
set xrange [0:500]

plot \
    'crucible_before_bw.1.log.hist' using 1:2 with lines lc rgb"blue" title 'before', \
    'crucible_after_bw.1.log.hist' using 1:2 with lines lc rgb"red" title 'after'

set output 'crucible_lat_histogram.png'
set xlabel 'nsec'
unset xrange

plot \
    'crucible_before_lat.1.log.hist' using 1:2 with lines lc rgb"blue" title 'before', \
    'crucible_after_lat.1.log.hist' using 1:2 with lines lc rgb"red" title 'after'

