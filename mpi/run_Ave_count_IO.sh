#!/bin/bash
#BATCH --job-name="mb_fread"
#SBATCH --output="Ave_Count_IO.%j.%N.out"
#SBATCH --partition=compute
##SBATCH --nodes=4
##SBATCH --ntasks-per-node=8
#SBATCH --export=ALL
#SBATCH -t 01:00:00
module load openmpi_ib/1.8.4

export WORK_DIR=`pwd`

#This job runs with 2 nodes, 24 cores per node for a total of 48 cores.
#ibrun in verbose mode will give binding detail

rm $WORK_DIR/nodes
srun -n $1 --ntasks-per-node=1 hostname >> $WORK_DIR/nodes


srun -n $1 --ntasks-per-node=1 cp ~/$4 /scratch/$USER/$SLURM_JOBID/$4
srun -n $1 chmod -R 777 /scratch/$USER/$SLURM_JOBID/
srun -n $1 --ntasks-per-node=1 ls /scratch/$USER/$SLURM_JOBID/

mpirun -np $3 --npernode $2 ./Ave_Count_IO /scratch/$USER/$SLURM_JOBID/$4

#ibrun -v  Ave_Count_IO-onenode


#sbatch --nodes 2 run_Ave_count_IO.sh 2 4 8 TestFile
