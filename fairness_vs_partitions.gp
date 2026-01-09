set terminal png size 2000,800 font ',15'
set key right
set output 'fairness_vs_partitions.png'
set yrange [0:0<*]
set xlabel 'Partition duration (%)'
set ylabel 'Fairly stabilized commands (%)'
set style data histograms
set style histogram clustered
set style fill solid 1.0 border lt -1
set boxwidth 0.9
set title 'Fairness vs Partitions'

set style line 11 lc rgb '#808080' lt 1 lw 3
set border back ls 11

set style line 12 lc rgb '#808080' lt 0 lw 1
set grid back ls 12

plot 'fairness_vs_partitions.dat' using 2:xtic(1) title 'f_{bfs} (total)' linecolor rgb "#377EB8", \
    '' using 4:xtic(1) title 'f_{fair} (total)' linecolor rgb "#4DAF4A",\
    '' using 5:xtic(1) title 'f_{fair} (minimum by process)' with linespoints linecolor rgb "#FF0000"