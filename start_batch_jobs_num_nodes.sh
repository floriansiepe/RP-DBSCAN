#!/usr/bin/env bash

# Iterate over node counts to measure scalability of the algorithm.
# This script submits SLURM jobs with varying numbers of nodes to test strong scaling.

# Check if run.slurm exists
if [ ! -f "./run.slurm" ]; then
  echo "Error: run.slurm not found!"
  exit 1
fi

# Number of nodes to iterate over (adjust as needed for your cluster)
node_counts=(1 2 4 8 16)

# Fixed number of partitions
num_partitions=128

# Create a grid of batch jobs
datasets=("densired_2.csv" "densired_3.csv" "densired_4.csv" "densired_5.csv" "activity.csv" "geolife_gps_data.csv" "twitter_processed.csv" "tng_50.csv")
dims=(2 3 4 5 3 3 2 3)
min_pts_values=(10 10 10 10 50 40 50 50)
eps_values=(0.15 0.15 0.15 0.15 0.15 70 0.15 6)

# Default values for other parameters
MIN_CELL_FACTOR=3
NUM_PARTITIONER=64
DBSCAN_IMPL="grid"
SAMPLER="random"

failed_count=0
success_count=0

echo "Starting scalability batch jobs over node counts: ${node_counts[*]}"

for num_nodes in "${node_counts[@]}"; do
  echo -e "\n--- Running with num_nodes=$num_nodes ---"
  for i in "${!datasets[@]}"; do
    dataset="${datasets[$i]}"
    min_pts="${min_pts_values[$i]}"
    eps="${eps_values[$i]}"
    dim="${dims[$i]}"

    exp_dir="/home/siepef/experiments/num_nodes/${num_nodes}"
    mkdir -p "$exp_dir"

    echo "Running dataset=$dataset dim=$dim eps=$eps minPts=$min_pts nodes=$num_nodes"

    sbatch --nodes="$num_nodes" --time=01:00:00 run.sh "/scratch_shared/siepef/datasets/$dataset" "$dim" "$eps" "$min_pts" "$num_partitions" "$exp_dir"
    exit_code=$?
    if [ $exit_code -ne 0 ]; then
      echo "-> FAILED: $dataset nodes=$num_nodes (exit $exit_code)"
      ((failed_count++))
    else
      echo "-> OK: $dataset nodes=$num_nodes"
      ((success_count++))
    fi
  done
done

echo -e "\nBatch jobs completed. Success: $success_count, Failed: $failed_count"

