#!/bin/bash

processesnb=16
experiment_duration=60
partitions_ratios=(0 0.2 0.5 0.8)
functions=(1 2 3)

data_file="fairness_vs_partitions.dat"

if [ -f $data_file ]; then
    echo "$data_file already exists. Exiting to avoid overwriting."
    exit 0
fi

touch $data_file

make build

for r in "${partitions_ratios[@]}"
do
    echo "Running experiment with partition of ratio $r"
    result="$r"
    for f in "${functions[@]}"  # run each reconciliation function
    do 
        sudo make run p=$processesnb d=$experiment_duration f=$f partition=$r

        fairlystabilized_txt=$(grep -E 'Number of fairly stabilized operations:' ./experiment/process1/process1.log)  # enough to check process 1 since all end in the same state
        fairlystabilized=($(echo $fairlystabilized_txt | grep -E -o "[0-9]+")) # 0 is the nb of fairly stabilized operations, 1 is the total nb of operations
        fairlystabilizedratio=$(echo "scale=3; 100 * ${fairlystabilized[0]} / ${fairlystabilized[1]}" | bc)

        minimumfair_txt=$(grep -E 'Less fair process had:' ./experiment/process1/process1.log)
        minimumfair=$(echo $minimumfair_txt | grep -E -o "[0-9]+") # nb of operations of the less fair process
        minimumfairratio=$(echo "scale=3; 100 * $minimumfair / ${fairlystabilized[1]}" | bc)

        result="$result $fairlystabilizedratio $minimumfairratio"
        sudo make stop
    done
    echo "$result" >> $data_file
done

gnuplot -persist fairness_vs_partitions.gp