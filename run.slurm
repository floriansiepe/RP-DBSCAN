#!/bin/bash
#SBATCH --job-name=RpDBSCANJob
#SBATCH --nodes=4
#SBATCH --tasks-per-node=1
#SBATCH --cpus-per-task=32
#SBATCH --output=rp-dbscan_output_%j.txt
#SBATCH --time=05:00:00
#SBATCH --mail-user=siepef@uni-marburg.de
#SBATCH --mail-type=END
#SBATCH --mem=128G

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

# --- Compute available memory per node and reserve a small chunk for OS ---
# Prefer SLURM-provided memory; otherwise fall back to /proc/meminfo
TOTAL_MEM_MB=0
if [ -n "$SLURM_MEM_PER_NODE" ]; then
    TOTAL_MEM_MB=$SLURM_MEM_PER_NODE
elif [ -n "$SLURM_MEM_PER_CPU" ] && [ -n "$SLURM_CPUS_ON_NODE" ]; then
    TOTAL_MEM_MB=$(( SLURM_MEM_PER_CPU * SLURM_CPUS_ON_NODE ))
elif [ -r /proc/meminfo ]; then
    # MemTotal is in kB
    mem_kb=$(awk '/MemTotal/ {print $2}' /proc/meminfo 2>/dev/null || echo 0)
    TOTAL_MEM_MB=$(( mem_kb / 1024 ))
else
    TOTAL_MEM_MB=64000  # fallback guess
fi

# Reserve at least 1 GiB or 5% of node memory (whichever is larger) for OS and daemons
RESERVE_MB=$(( TOTAL_MEM_MB / 20 ))
if [ "$RESERVE_MB" -lt 1024 ]; then
    RESERVE_MB=1024
fi

ALLOC_MEM_MB=$(( TOTAL_MEM_MB - RESERVE_MB ))
if [ "$ALLOC_MEM_MB" -lt 1024 ]; then
    ALLOC_MEM_MB=$(( TOTAL_MEM_MB * 80 / 100 ))
fi

# Determine executor memory and overhead: reserve at least 10% or 384MB for overhead (off-heap, native)
MEMORY_OVERHEAD_MB=$(( ALLOC_MEM_MB / 10 ))
if [ "$MEMORY_OVERHEAD_MB" -lt 384 ]; then
    MEMORY_OVERHEAD_MB=384
fi

# Executor (JVM heap) memory = allocated memory minus overhead
EXECUTOR_HEAP_MB=$(( ALLOC_MEM_MB - MEMORY_OVERHEAD_MB ))
if [ "$EXECUTOR_HEAP_MB" -lt 512 ]; then
    # ensure a sane minimum
    EXECUTOR_HEAP_MB=$(( ALLOC_MEM_MB * 80 / 100 ))
fi

# Export Spark worker memory and cores so start-worker uses these values
export SPARK_WORKER_MEMORY="${ALLOC_MEM_MB}m"
# Use all CPUs allocated to the SLURM task for the Spark worker
export SPARK_WORKER_CORES=${SLURM_CPUS_PER_TASK:-$SLURM_CPUS_ON_NODE}

# Compute cores settings for submission
EXECUTOR_CORES=${SPARK_WORKER_CORES:-1}
TOTAL_EXECUTOR_CORES=$(( EXECUTOR_CORES * NUM_NODES ))

echo "Node $(hostname): TOTAL_MEM_MB=${TOTAL_MEM_MB}MB, RESERVED=${RESERVE_MB}MB, ALLOC_MEM_MB=${ALLOC_MEM_MB}MB, EXECUTOR_HEAP_MB=${EXECUTOR_HEAP_MB}MB, MEMORY_OVERHEAD_MB=${MEMORY_OVERHEAD_MB}MB, SPARK_WORKER_CORES=${SPARK_WORKER_CORES}, TOTAL_EXECUTOR_CORES=${TOTAL_EXECUTOR_CORES}"

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

    # Create spark-defaults.conf with executor/driver defaults. We'll also pass explicit
    # --conf flags during spark-submit to ensure executors use the per-node allocation.
    cat <<EOF > "$SPARK_CONF_DIR/spark-defaults.conf"
spark.shuffle.service.enabled       true
spark.shuffle.service.port          7337
# Note: spark.executor.memory is set dynamically in the submission command in this script.
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
# start shuffle service and worker. start-worker will respect SPARK_WORKER_MEMORY and CORES env vars
$SPARK_HOME/sbin/start-shuffle-service.sh
sleep 3 # Give the service a moment to start.

# Provide the master URL for the worker
$SPARK_HOME/sbin/start-worker.sh "spark://$MASTER_NODE_HOSTNAME:7077"
echo "Node $(hostname): Shuffle Service and Worker daemons started."

# --- 5. Submit the Application (Master Node Only) ---
if [ $GLOBAL_RANK == 0 ]; then
    echo "Waiting for all workers to register with the Master..."
    sleep 10

    echo "Submitting Spark application..."
    # Configure executors to use the full per-node allocation (one executor per node)
    EXECUTOR_MEMORY="${ALLOC_MEM_MB}m"
    EXECUTOR_CORES=${SPARK_WORKER_CORES:-1}
    EXECUTOR_INSTANCES=${NUM_NODES}
    DRIVER_MEMORY="4g"

    echo "Spark submit settings: --conf spark.executor.memory=${EXECUTOR_HEAP_MB}m --conf spark.executor.memoryOverhead=${MEMORY_OVERHEAD_MB} --conf spark.executor.cores=${EXECUTOR_CORES} --total-executor-cores ${TOTAL_EXECUTOR_CORES} --driver-memory=${DRIVER_MEMORY}"

    $SPARK_HOME/bin/spark-submit \
      --master "spark://$MASTER_NODE_HOSTNAME:7077" \
      --deploy-mode client \
      --class dm.kaist.main.MainDriver \
      --driver-memory ${DRIVER_MEMORY} \
      --conf spark.executor.memory=${EXECUTOR_HEAP_MB}m \
      --conf spark.executor.memoryOverhead=${MEMORY_OVERHEAD_MB} \
      --conf spark.executor.cores=${EXECUTOR_CORES} \
      --total-executor-cores ${TOTAL_EXECUTOR_CORES} \
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
