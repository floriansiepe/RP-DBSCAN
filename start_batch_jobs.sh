#!/usr/bin/env bash

# Check if run.sh exists
if [ ! -f "./run.sh" ]; then
  echo "Error: run.sh not found!"
  exit 1
fi

num_partitions=128

# Create a grid of batch jobs
datasets=("densired_2.csv" "densired_3.csv" "densired_4.csv" "densired_5.csv" "activity.csv" "geolife_gps_data.csv" "twitter_processed.csv" "tng_50.csv") # simple-gps-points-120312.txt
dims=(2 3 4 5 3 3 2 3)
min_pts_values=(10 10 10 10 50 40 50 50) # 100
eps_values=(
  "0.05 0.1 0.15 0.2 0.25 0.3"
  "0.05 0.1 0.15 0.2 0.25 0.3"
  "0.05 0.1 0.15 0.2 0.25 0.3"
  "0.05 0.1 0.15 0.2 0.25 0.3"
  "0.05 0.1 0.15 0.2 0.25 0.3"
  "10 40 70 100 130 160"
  "0.05 0.1 0.15 0.2 0.25 0.3"
  "2 4 6 8 10 12"
  # "0.02, 0.03, 0.04, 0.05, 0.06, 0.07"
)

failed_count=0
success_count=0

echo "Starting batch jobs..."
for i in "${!datasets[@]}"; do
    dataset="${datasets[$i]}"
    min_pts="${min_pts_values[$i]}"
    eps_string="${eps_values[$i]}"
    dim="${dims[$i]}"
    read -r -a eps_array <<< "$eps_string"
    for eps in "${eps_array[@]}"; do
      ./run.sh "/scratch_shared/siepef/datasets/$dataset" "$dim" "$eps" "$min_pts" "$num_partitions" /home/siepef/experiments/eps
      exit_code=$?
      if [ $exit_code -ne 0 ]; then
        ((failed_count++))
      else
        ((success_count++))
      fi
    done
done

echo "Batch jobs completed. Success: $success_count, Failed: $failed_count"
