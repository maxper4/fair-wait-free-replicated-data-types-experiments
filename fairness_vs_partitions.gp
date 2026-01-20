set terminal pict2e size 3.6in,1.8in font ",8"
#set key right
set key outside top center columns 2 width 7
set output 'fairness_vs_partitions_removewins.tex'
set yrange [0:0<*]
set xlabel 'Partition duration (ratio)'
set ylabel 'Successful commands (\%)'
set style data histograms
set style histogram clustered
set style fill solid 1.0 border lt -1
set boxwidth 0.9
#set title 'Fairness vs Partitions'

set style line 11 lc rgb '#808080' lt 1 lw 3
set border back ls 11

set style line 12 lc rgb '#808080' lt 0 lw 1
set grid back ls 12

plot 'fairness_vs_partitions_removewins.dat' using 2:xtic(1) title '\fbfs' linecolor rgb "#377EB8", \
    '' using 4:xtic(1) title '\ffair' linecolor rgb "#4DAF4A",
    #'' using 6:xtic(1) title '\fcrdt' linecolor rgb "#FF0000"