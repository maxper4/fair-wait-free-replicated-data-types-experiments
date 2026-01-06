set terminal png size 2000,800 font ',15'
set key outside
set output 'stability_vs_processes.png'
#set auto x
set auto y
set xlabel 'Number of processes'
set ylabel 'Average number of reordering per operation'
#unset key
#set key title ''
set style data histograms
set style histogram cluster
set style fill solid 1.0 border lt -1
set boxwidth 0.9
set xtic scale 0
set title 'Stability vs Processes'

plot 'stability_vs_processes.dat' using 2:xtic(1) title 'f_{bfs}', '' using 3:xtic(1) title 'f_{fair}'
