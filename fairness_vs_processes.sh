#!/bin/bash

processesnb=(4 8 12 16)
experiment_duration=300
partitions_ratio=0.33
functions=(1 2)
data_type=1

data_file="fairness_vs_processes.dat"

if [ -f $data_file ]; then
    echo "$data_file already exists. Exiting to avoid overwriting."
    exit 0
fi

touch $data_file

make build

for n in "${processesnb[@]}"
do
    echo "Running experiment with $n processes"
    result="$n"
    for f in "${functions[@]}"  # run each reconciliation function
    do 
        sudo make run p=$n d=$experiment_duration f=$f partition=$partitions_ratio t=$data_type

        minimumfair_txt=$(grep -E 'Less fair process had:' ./experiment/process1/process1.log)
        minimumfair=$(echo $minimumfair_txt | grep -E -o "[0-9]+") # nb of operations of the less fair process
        maximumfair_txt=$(grep -E 'Most fair process had:' ./experiment/process1/process1.log)
        maximumfair=$(echo $maximumfair_txt | grep -E -o "[0-9]+") # nb of operations of the more fair process
        ratio=$(echo "scale=3; $minimumfair / $maximumfair" | bc) # should be 1 if uniformly fair

        result="$result $ratio"
        sudo make stop
    done
    echo "$result" >> $data_file
done

gnuplot -persist fairness_vs_processes.gp