set terminal png size 2000,800 font ',15'
set key outside
set output 'computation_vs_commands.png'
set auto x
set auto y
set logscale y 2
set xlabel 'State size (number of commands)'
set ylabel 'Average read duration (ms)'
#unset key
#set key title ''
set style line 1 lt 1 lw 1.5 pt 3 linecolor rgb '#2b63ff'
set title 'Computation vs State Size'

plot 'computation_vs_commands.dat' using 2:xtic(1) title 'f_{bfs}' with linespoints ls 1, '' using 3:xtic(1) title 'f_{fair}' with linespoints ls 1
