#!/bin/bash

processesnb=16
experiment_duration=60
functions=(1 2)
commands=(10 100 500 1000)

data_file="computation_vs_commands.dat"

if [ -f $data_file ]; then
    echo "$data_file already exists. Exiting to avoid overwriting."
    exit 0
fi

touch $data_file

make build

results=()
for c in "${commands[@]}"
do 
    results["$c"]="$c"
done

for f in "${functions[@]}"
do 
    sudo make run p=$processesnb d=$experiment_duration f=$f

    sums=()
    for c in "${commands[@]}"
    do 
        sums["$c"]=0
    done

    for i in $(seq 1 $processesnb)
    do
        for c in "${commands[@]}"
        do 
            time_txt=$(grep -E "Computation time for state of length $c:" ./experiment/process$i/process$i.log)
            t=($(echo $time_txt | grep -E -o "[0-9]+")) # 0 is state length, 1 is time
            sums["$c"]=$(echo "${sums["$c"]} + ${t[1]}" | bc)
        done
    done

    for c in "${commands[@]}"
    do 
        sum=${sums["$c"]}
        avg=$(echo "scale=3; $sum / $processesnb" | bc)
        results["$c"]="${results["$c"]} $avg"
    done

    sudo make stop
done

for c in "${commands[@]}"
do 
    result=${results["$c"]} 
    echo "$result" >> $data_file
done

gnuplot -persist computation_vs_commands.gp