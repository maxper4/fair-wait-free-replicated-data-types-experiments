set terminal png size 2000,800 font ',15'
set key right
set output 'fairness_vs_processes.png'
set yrange [0:1]
set xlabel 'Number of processes'
set ylabel 'Ratio minimum/maximum of fairly stabilized commands by process, after partition'
set style data histograms
set style histogram clustered
set style fill solid 1.0 border lt -1
set boxwidth 0.9
set title 'Fairness vs Processes, after network partition'

set style line 11 lc rgb '#808080' lt 1 lw 3
set border back ls 11

set style line 12 lc rgb '#808080' lt 0 lw 1
set grid back ls 12

plot 'fairness_vs_processes.dat' using 2:xtic(1) title 'f_{bfs}' linecolor rgb "#377EB8", \
    '' using 3:xtic(1) title 'f_{fair}' linecolor rgb "#4DAF4A",\