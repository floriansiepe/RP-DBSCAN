#!/bin/bash
#SBATCH --job-name=RpDBSCANJob
#SBATCH --nodes=4
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=16
#SBATCH --output=rp-dbscan_output_%j.txt
#SBATCH --time=05:00:00
#SBATCH --mail-user=siepef@uni-marburg.de
#SBATCH --mail-type=END
#SBATCH --mem=128G
##SBATCH --exclusive
# Note: Cluster orchestration delegated to run_inner.sh launched via srun.

# Load environment once (inner tasks will also load what they need if required)
module purge
module load openjdk/21.0.2

# Capture arguments to forward to inner script
DATASET=$1; DIM=$2; EPS=$3; MINPTS=$4; NUM_PARTITIONS=$5; EXP_DIR=$6; OUT=$7; RHO=$8

# Set spark env
SPARK_EXECUTOR_MEMORY=100g

# Basic validation
if [ -z "$RHO" ]; then
  echo "[ERROR] Missing arguments. Expected 8 positional args: dataset dim eps minPts numPartitions expDir out rho"; exit 1; fi

# Show allocation summary
echo "[ORCH] JobID=$SLURM_JOB_ID Nodes=$SLURM_JOB_NODELIST NNodes=$SLURM_NNODES NTasks=$SLURM_NTASKS Cores/Task=$SLURM_CPUS_PER_TASK"

# Ensure inner script exists (defensive)
if [ ! -f "run_inner.sh" ]; then
  echo "[ERROR] run_inner.sh not found in working directory $(pwd)"; exit 1
fi

# Launch one task per node (task layout already allocated). Use --label for clearer logs.
# Forward positional args.
set -e
srun bash run_inner.sh "$DATASET" "$DIM" "$EPS" "$MINPTS" "$NUM_PARTITIONS" "$EXP_DIR" "$OUT" "$RHO"
rc=$?
set +e
echo "[ORCH] srun completed rc=$rc"
exit $rc
