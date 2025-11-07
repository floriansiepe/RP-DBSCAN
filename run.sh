#!/usr/bin/env bash
# Get dataset, dim, eps, minPts and num_partitions from command line arguments
DATASET=${1}
DIM=${2}
EPS=${3}
MINPTS=${4}
NUM_PARTITIONS=${5}
EXP_DIR=${6}
RHO="0.01"
OUT="out.csv"

# Check if all arguments are provided
if [ -z "$DATASET" ] || [ -z "$DIM" ] || [ -z "$EPS" ] || [ -z "$MINPTS" ] || [ -z "$NUM_PARTITIONS" ] || [ -z "$EXP_DIR" ]; then
  echo "Usage: $0 <dataset> <dim> <eps> <minPts> <num_partitions> <exp_dir>"
  exit 1
fi

sbatch run.slurm "$DATASET" "$DIM" "$EPS" "$MINPTS" "$NUM_PARTITIONS" "$EXP_DIR" "$OUT" "$RHO"

exit 0


