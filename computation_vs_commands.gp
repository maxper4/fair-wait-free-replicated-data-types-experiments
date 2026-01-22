set terminal pict2e size 3.6in,1.8in font ",8"
set key left width 7
set output 'computation_vs_commands.tex'
set auto x
set auto y
#set logscale y 2
set xlabel 'State size (number of commands)' offset 0,-1
set ylabel 'Average read duration ($\mu s$)' offset -1.5,0
set xtics rotate by 45 offset -3,-1.4
#unset key
#set key title ''
set style line 1 lt 1 lw 1.5 pt 3 linecolor rgb '#2b63ff'
#set title 'Computation vs State Size'

set style line 11 lc rgb '#808080' lt 1 lw 3
set border back ls 11

set style line 12 lc rgb '#808080' lt 0 lw 1
set grid back ls 12

plot 'computation_vs_commands.dat' using 2:xtic(1) title '\fbfs' with linespoints ls 1 linecolor rgb "#377EB8",\
    '' using 3:xtic(1) title '\ffair' with linespoints ls 1 linecolor rgb "#4DAF4A",
    #'' using 4:xtic(1) title '\fcrdt' with linespoints ls 1 linecolor rgb "#E41A1C"
