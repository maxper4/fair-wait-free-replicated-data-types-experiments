set terminal png size 2000,800 font ',15'
set key outside
set output 'fairness_vs_partitions.png'
#set auto x
set auto y
set yrange [0:0<*]
set xlabel 'Partition (duration %)'
set ylabel 'Fairly stabilized commands (%)'
#unset key
#set key title ''
set style data histograms
set style histogram clustered
set style fill solid 1.0 border lt -1
set boxwidth 0.9
set xtic scale 0
set title 'Fairness vs Partitions'

plot 'fairness_vs_partitions.dat' using 2:xtic(1) title 'f_{bfs} (total)', '' using 3:xtic(1) title 'f_{bfs} (minimum by process)', 'fairness_vs_partitions.dat' using 4:xtic(1) title 'f_{fair} (total)', '' using 5:xtic(1) title 'f_{fair} (minimum by process)'