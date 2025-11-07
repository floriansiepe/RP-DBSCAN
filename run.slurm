#!/bin/bash
#SBATCH --job-name=RpDBSCANJob
#SBATCH --nodes=4
#SBATCH --tasks-per-node=1
#SBATCH --cpus-per-task=32
#SBATCH --output=rp-dbscan_output_%j.txt
#SBATCH --time=01:00:00
##SBATCH --mail-user=siepef@uni-marburg.de
#SBATCH --mail-type=END
#SBATCH --mem=64G

# 1. Set up the environment
# It's good practice to load the required Java module.
# The name of the module might differ on your system.
module purge
module load openjdk/21.0.2

# --- 1. Environment Setup ---
DATASET=${1}
DIM=${2}
EPS=${3}
MINPTS=${4}
NUM_PARTITIONS=${5}
EXP_DIR=${6}
OUT=${7}
RHO=${8}

# Get SLURM and system information
GLOBAL_RANK=$SLURM_PROCID
NUM_NODES=$SLURM_NTASKS
MASTER_NODE_HOSTNAME=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -n 1)

# Define paths on the SHARED filesystem early so we can place a copy of the submit
# script on a shared location before re-launching with srun.
SCRATCH_DIR="/scratch_shared/siepef"
SHARED_SCRIPT="$SCRATCH_DIR/slurm_job_${SLURM_JOBID}_run.sh"

# Debug: print SLURM environment variables so we can see how the script was started
echo "SLURM info: JOBID=$SLURM_JOBID, STEP_ID=$SLURM_STEP_ID, PROCID=$SLURM_PROCID, NTASKS=$SLURM_NTASKS, NODES=$SLURM_NNODES, NTASKS_PER_NODE=$SLURM_TASKS_PER_NODE"

# If this script was submitted via sbatch it runs on the controller node only; re-launch it
# across all allocated tasks so each node runs the same script (one task per node).
# Use SLURM_STEP_ID as a guard to avoid recursive re-exec inside srun.
if [ -z "$SLURM_STEP_ID" ] && [ "${NUM_NODES:-0}" -gt 1 ]; then
    echo "Copying submit script to shared location ($SHARED_SCRIPT) and re-launching script on all nodes with srun..."
    mkdir -p "$SCRATCH_DIR" || true
    # Copy the currently executing script (which may be the sbatch spool file) to a shared path
    if cp "$0" "$SHARED_SCRIPT" 2>/dev/null; then
        chmod a+x "$SHARED_SCRIPT" || true
        srun --nodes=$SLURM_NNODES --ntasks=$NUM_NODES --ntasks-per-node=1 \
             --cpus-per-task=$SLURM_CPUS_PER_TASK --export=ALL bash "$SHARED_SCRIPT" "$@"
        exit $?
    else
        echo "Warning: failed to copy $0 to $SHARED_SCRIPT. Falling back to attempting srun with $0 path (may fail on compute nodes)."
        srun --nodes=$SLURM_NNODES --ntasks=$NUM_NODES --ntasks-per-node=1 \
             --cpus-per-task=$SLURM_CPUS_PER_TASK --export=ALL bash "$0" "$@"
        exit $?
    fi
fi

# Define paths on the SHARED filesystem
SPARK_LOG_DIR="$SCRATCH_DIR/spark_job_logs_${SLURM_JOBID}"
SPARK_CONF_DIR="$SCRATCH_DIR/spark_job_conf_${SLURM_JOBID}"
BARRIER_DIR="$SCRATCH_DIR/spark_job_barrier_${SLURM_JOBID}" # Directory for our sync files

# Export key Spark environment variables so all Spark scripts use them.
export SPARK_LOG_DIR
export SPARK_CONF_DIR
export SPARK_WORKER_DIR="$SCRATCH_DIR/spark_worker_data_${SLURM_JOBID}"

echo "Node $(hostname): Rank=$GLOBAL_RANK, Master=$MASTER_NODE_HOSTNAME, Total Nodes=$NUM_NODES"

# --- 2. Barrier Function ---
# A function to synchronize all nodes.
function barrier_sync() {
    local barrier_name=$1
    local timeout=${2:-300}  # default timeout in seconds (optional second arg)
    local barrier_path="$BARRIER_DIR/$barrier_name"

    # Ensure barrier dir exists (on shared filesystem)
    mkdir -p "$barrier_path" || { echo "Node $(hostname): ERROR creating barrier path $barrier_path"; return 1; }

    # Create an atomic per-node marker. mkdir is atomic on POSIX filesystems and avoids races
    # and some NFS caching oddities; fall back to touch if mkdir not possible.
    local marker_dir="$barrier_path/node_${GLOBAL_RANK}.ready"
    if ! mkdir "$marker_dir" 2>/dev/null; then
        # fallback to a file marker (still ok)
        : > "$marker_dir" || { echo "Node $(hostname): ERROR creating marker $marker_dir"; return 1; }
    fi

    echo "Node $(hostname): Reached barrier '$barrier_name'. Waiting for $NUM_NODES nodes (timeout=${timeout}s)..."

    local start_ts=$(date +%s)
    local prev_count=-1

    while true; do
        # Try to reduce NFS metadata caching problems by forcing a local sync and re-reading
        sync 2>/dev/null || true

        # Count node markers. We include both files and directories that start with node_.
        # Use a glob to avoid issues with ls formatting; fall back safely if empty.
        local count=0
        if [ -d "$barrier_path" ]; then
            # Use find for robust counting across different ls behaviors
            count=$(find "$barrier_path" -maxdepth 1 -mindepth 1 -name 'node_*' | wc -l || true)
        fi

        if [ "$count" -ge "$NUM_NODES" ]; then
            echo "Node $(hostname): All $count nodes reached barrier '$barrier_name'."
            break
        fi

        if [ "$count" -ne "$prev_count" ]; then
            echo "Node $(hostname): Barrier '$barrier_name' progress: $count/$NUM_NODES"
            prev_count=$count
        fi

        local now_ts=$(date +%s)
        local elapsed=$((now_ts - start_ts))
        if [ "$elapsed" -ge "$timeout" ]; then
            echo "Node $(hostname): ERROR: Timeout waiting at barrier '$barrier_name' after ${elapsed}s (expected $NUM_NODES)."
            echo "Barrier path = $barrier_path"
            echo "Barrier listing (detailed):"
            ls -la "$barrier_path" || true
            echo "Environment: GLOBAL_RANK=$GLOBAL_RANK, NUM_NODES=$NUM_NODES, SLURM_JOB_NODELIST=$SLURM_JOB_NODELIST"
            # Return non-zero so callers can decide how to handle it
            return 1
        fi

        sleep 1
    done

    echo "Node $(hostname): Passed barrier '$barrier_name'."
    return 0
}

# --- 3. Create Cluster Configuration (Master Node Only) ---
if [ $GLOBAL_RANK == 0 ]; then
    echo "Master node creating shared configuration and directories..."
    mkdir -p "$SPARK_CONF_DIR"
    mkdir -p "$SPARK_LOG_DIR"
    mkdir -p "$SPARK_WORKER_DIR"
    mkdir -p "$BARRIER_DIR"

    cat <<EOF > "$SPARK_CONF_DIR/spark-defaults.conf"
spark.shuffle.service.enabled       true
spark.shuffle.service.port          7337
EOF
    echo "Configuration created in $SPARK_CONF_DIR"
fi

# Use the barrier to wait for the config to be created.
barrier_sync "config_ready"

# --- 4. Start Daemons ---
if [ $GLOBAL_RANK == 0 ]; then
    echo "Starting Spark Master..."
    $SPARK_HOME/sbin/start-master.sh

    echo "Waiting for Spark Master to open port 7077..."
    while ! (echo > /dev/tcp/$MASTER_NODE_HOSTNAME/7077) &>/dev/null; do
      sleep 1
    done
    echo "Spark Master is ready."
fi

# Use the barrier to ensure the Master is fully up before workers connect.
barrier_sync "master_ready"

# ALL nodes start their services.
echo "Node $(hostname): Starting daemons..."
$SPARK_HOME/sbin/start-shuffle-service.sh
sleep 3 # Give the service a moment to start.

$SPARK_HOME/sbin/start-worker.sh "spark://$MASTER_NODE_HOSTNAME:7077"
echo "Node $(hostname): Shuffle Service and Worker daemons started."

# --- 5. Submit the Application (Master Node Only) ---
if [ $GLOBAL_RANK == 0 ]; then
    echo "Waiting for all workers to register with the Master..."
    sleep 10

    echo "Submitting Spark application..."
    $SPARK_HOME/bin/spark-submit \
      --master "spark://$MASTER_NODE_HOSTNAME:7077" \
      --deploy-mode client \
      --class dm.kaist.main.MainDriver \
      /home/siepef/code/RP-DBSCAN/target/rp-dbscan-1.0-SNAPSHOT.jar \
      -i "$DATASET" -o "$OUT" -rho "$RHO" -dim "$DIM" -eps "$EPS" -minPts "$MINPTS" -np "$NUM_PARTITIONS" -M "$EXP_DIR"

    if [ $? -eq 0 ]; then
      echo "Success: Spark job completed."
    else
      echo "Error: Spark job failed. Check logs in $SPARK_LOG_DIR"
    fi
fi

# --- 6. Teardown and Cleanup ---
if [ $GLOBAL_RANK == 0 ]; then
    echo "Job finished. Waiting before shutting down cluster..."
    sleep 20
    echo "Shutting down Spark cluster."
    $SPARK_HOME/sbin/stop-all.sh
    # Manually stop shuffle services on all nodes.
    srun --jobid=$SLURM_JOBID $SPARK_HOME/sbin/stop-shuffle-service.sh
    # Clean up the temporary directories.
    echo "Cleaning up shared directories..."
    rm -rf "$SPARK_LOG_DIR" "$SPARK_CONF_DIR" "$SPARK_WORKER_DIR" "$BARRIER_DIR"
    # Remove the shared copy of the submit script if it was created.
    rm -f "$SHARED_SCRIPT"
fi
