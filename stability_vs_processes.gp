set terminal pict2e size 3.6in,1.6in font ",8"
set key inside top left columns 2 width 7
set output 'stability_vs_processes_nosem.tex'
set logscale y 2
set yrange [0:*]
set xlabel 'Number of processes'
#set ylabel "Average number of change\nof result per command"
set ylabel "Average number of reordering\nper command" offset -2,0
set style data histograms
set style histogram cluster
set style fill solid 1.0 border lt -1
set boxwidth 0.9
#set title 'Stability vs Processes'

set style line 11 lc rgb '#808080' lt 1 lw 3
set border back ls 11

set style line 12 lc rgb '#808080' lt 0 lw 1
set grid back ls 12

plot 'stability_vs_processes_nosem.dat' using 2:xtic(1) title '\fbfs' linecolor rgb "#377EB8",  \
    '' using 3:xtic(1) title '\ffair' linecolor rgb "#4DAF4A", \
    #'' using 4:xtic(1) title '\fcrdt' linecolor rgb "#E41A1C" 
