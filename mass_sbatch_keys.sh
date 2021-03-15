#!/bin/bash

for i in 1 1 1 1 1 1	#number of runs
do
	for j in 1 2 4	#number of nodes
	do
		for k in 2 4 8 16	#number of cores/tasks per node
		do
			sbatch --time=15:00:00 --constraint=elves --mem=60G --ntasks-per-node=$k --nodes=$j keys.sh $(($j*$k)) $j $k
		done
	done
done
