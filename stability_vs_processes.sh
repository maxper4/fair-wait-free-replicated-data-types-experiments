#!/bin/bash

processesnb=(4 8 12 16)
experiment_duration=30
experiment_types=(1 2)

data_file="stability_vs_processes.dat"

if [ -f $data_file ]; then
    echo "$data_file already exists. Exiting to avoid overwriting."
    exit 0
fi

touch $data_file

make build

for nb in "${processesnb[@]}"
do
    echo "Running experiment with $nb processes"
    result="$nb"
    for e in "${experiment_types[@]}"  # run each reconciliation function
    do 
        sudo make run p=$nb d=30 e=$e

        sum_reorgs=0
        for i in $(seq 1 $nb)
        do
            reorgs_txt=$(grep -E 'Average reorderings by operation for process '$i':' ./experiment/process$i/process$i.log)
            reorgs=$(echo $reorgs_txt | grep -E -o "[0-9]+.[0-9]{3}")
            echo "$reorgs"
            sum_reorgs=$(echo "$sum_reorgs + $reorgs" | bc)
        done
        avg_reorgs=$(echo "scale=3; $sum_reorgs / $nb" | bc)

        result="$result $avg_reorgs"
        sudo make stop
    done
    echo "$result" >> $data_file
done

gnuplot -persist stability_vs_processes.gp