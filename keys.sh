#!/bin/bash 
i=$1
j=$2	#number of nodes
k=$3	#number of cores/tasks per node
module load OpenMPI
mpirun -n ${i} ./keys_mpi_star ${j} ${k}
mpirun -n ${i} ./keys_mpi_ring ${j} ${k}
mpirun -n ${i} ./keys_mpi_queue ${j} ${k}