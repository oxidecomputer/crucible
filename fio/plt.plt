set terminal png nocrop enhanced size 1920,1080

set datafile separator ','

# time (msec), value, data direction, block size (bytes), offset (bytes)
set xlabel 'msec'
#set logscale y
#set yrange [0:4000]

# Bandwidth log: Value is in KiB/sec
set ylabel 'KiB/sec'

set output 'crucible_bw.png'
plot 'crucible_bw.1.log' using 1:2 title 'Crucible'

# Latency log: Value is latency in nsecs

unset xrange
#set yrange [0:300000000]
unset logscale y
set ylabel 'nsec'

set output 'crucible_lat.png'
set ylabel 'KiB/sec'
plot 'crucible_lat.1.log' using 1:2 title 'Crucible'

# histograms

set xlabel 'KiB/sec'
set ylabel 'frequency'

#unset yrange

set output 'crucible_bw_histogram.png'
set xlabel 'KiB/sec'

plot 'crucible_bw.1.log.hist' using 1:2 with lines lc rgb"red" title 'Crucible'

set output 'crucible_lat_histogram.png'
set xlabel 'nsec'

plot 'crucible_lat.1.log.hist' using 1:2 with lines lc rgb"red" title 'crucible'

