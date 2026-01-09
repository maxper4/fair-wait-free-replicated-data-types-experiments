set terminal png size 2000,800 font ',15'
set key left
set output 'computation_vs_commands.png'
set auto x
set auto y
#set logscale y 2
set xlabel 'State size (number of commands)'
set ylabel 'Average read duration (ms)'
#unset key
#set key title ''
set style line 1 lt 1 lw 1.5 pt 3 linecolor rgb '#2b63ff'
set title 'Computation vs State Size'

set style line 11 lc rgb '#808080' lt 1 lw 3
set border back ls 11

set style line 12 lc rgb '#808080' lt 0 lw 1
set grid back ls 12

plot 'computation_vs_commands.dat' using 2:xtic(1) title 'f_{bfs}' with linespoints ls 1 linecolor rgb "#377EB8",\
    '' using 3:xtic(1) title 'f_{fair}' with linespoints ls 1 linecolor rgb "#4DAF4A", \
    '' using 4:xtic(1) title 'f_{crdt}' with linespoints ls 1 linecolor rgb "#E41A1C"
