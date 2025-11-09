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
# Note: Running without --exclusive. Isolation is achieved via per-job dynamic ports
# and separate config/log/worker directories. Be aware of potential resource contention
# if other jobs share the same nodes.

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

# Shared scratch root
SCRATCH_DIR="/scratch_shared/siepef"
SHARED_SCRIPT="$SCRATCH_DIR/slurm_job_${SLURM_JOBID}_run.sh"

# --- Added: Per-job isolated directories ---
SPARK_CONF_DIR="$SCRATCH_DIR/spark_conf_${SLURM_JOB_ID}"
SPARK_LOG_DIR="$SCRATCH_DIR/spark_logs_${SLURM_JOB_ID}"
SPARK_WORKER_DIR="$SCRATCH_DIR/spark_worker_${SLURM_JOB_ID}"
BARRIER_DIR="$SCRATCH_DIR/barrier_${SLURM_JOB_ID}"
SPARK_PID_DIR="$SCRATCH_DIR/spark_pids_${SLURM_JOB_ID}"
export SPARK_PID_DIR SPARK_CONF_DIR

# --- Added: Dynamic port selection based on job id ---
# Choose a base port in a high, non-privileged range. We mod into a span to avoid overflow.
BASE_PORT=$((20000 + (SLURM_JOB_ID % 20000)))
# Helper to find a free port (avoid collision if multiple jobs map to same BASE_PORT)
pick_free_port() {
  local candidate=$1
  local limit=$((candidate+200))
  while [ $candidate -lt $limit ]; do
    if ! (echo > /dev/tcp/127.0.0.1/$candidate) &>/dev/null; then
      echo $candidate
      return 0
    fi
    candidate=$((candidate+1))
  done
  # Fallback: just return original if none found (unlikely)
  echo $1
}
SPARK_MASTER_PORT=$(pick_free_port $BASE_PORT)
SPARK_UI_PORT=$((SPARK_MASTER_PORT + 2))
# Removed shuffle port (external shuffle service disabled for stability)

# Export so Spark scripts can pick them up
export SPARK_MASTER_PORT SPARK_UI_PORT SPARK_MASTER_HOST="$MASTER_NODE_HOSTNAME"

# Cleanup function to ensure cluster teardown even on failure
function cleanup() {
  if [ "$GLOBAL_RANK" -eq 0 ]; then
    echo "[CLEANUP] Stopping Spark daemons for job $SLURM_JOB_ID";
    if [ -n "$SPARK_HOME" ]; then
      $SPARK_HOME/sbin/stop-master.sh || true
      # Stop workers on all nodes explicitly (scoped to allocation)
      srun --jobid=$SLURM_JOBID $SPARK_HOME/sbin/stop-worker.sh || true
    fi
    echo "[CLEANUP] Removing per-job directories";
    rm -rf "$SPARK_CONF_DIR" "$SPARK_LOG_DIR" "$SPARK_WORKER_DIR" "$BARRIER_DIR" "$SPARK_PID_DIR" || true
    rm -f "$SHARED_SCRIPT" || true
  fi
}
trap cleanup EXIT

# --- 2. Lightweight Resource Setup ---
# Rely on SLURM allocation; let Spark use all provided cores and memory.
if [ -n "$SLURM_MEM_PER_NODE" ]; then
  export SPARK_WORKER_MEMORY="${SLURM_MEM_PER_NODE}m"
fi
export SPARK_WORKER_CORES=${SLURM_CPUS_PER_TASK:-$SLURM_CPUS_ON_NODE}

# --- Barrier Function ---
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

    local start_ts
    start_ts=$(date +%s)
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

        local now_ts
        now_ts=$(date +%s)
        local elapsed=$((now_ts - start_ts))
        if [ "$elapsed" -ge "$timeout" ]; then
            echo "Node $(hostname): ERROR timeout at barrier '$barrier_name' after ${elapsed}s"
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
    mkdir -p "$SPARK_CONF_DIR" "$SPARK_LOG_DIR" "$SPARK_WORKER_DIR" "$BARRIER_DIR" "$SPARK_PID_DIR"
    cat <<EOF > "$SPARK_CONF_DIR/spark-defaults.conf"
spark.ui.port                       ${SPARK_UI_PORT}
spark.eventLog.enabled              false
# External shuffle disabled to avoid executor registration failures in standalone mode
spark.shuffle.service.enabled       false
EOF
    echo "Configuration created in $SPARK_CONF_DIR (MasterPort=$SPARK_MASTER_PORT UI=$SPARK_UI_PORT)"
fi

# Use the barrier to wait for the config to be created.
barrier_sync "config_ready"

# --- 4. Start Daemons ---
if [ $GLOBAL_RANK == 0 ]; then
    echo "Starting Spark Master on $MASTER_NODE_HOSTNAME:$SPARK_MASTER_PORT (UI $SPARK_UI_PORT)..."
    $SPARK_HOME/sbin/start-master.sh --host "$MASTER_NODE_HOSTNAME" --port "$SPARK_MASTER_PORT" --webui-port "$SPARK_UI_PORT"
    echo "Waiting for Spark Master port $SPARK_MASTER_PORT to open..."
    while ! (echo > /dev/tcp/$MASTER_NODE_HOSTNAME/$SPARK_MASTER_PORT) &>/dev/null; do sleep 1; done
    echo "Spark Master is ready."
fi

# Use the barrier to ensure the Master is fully up before workers connect.
barrier_sync "master_ready"

# ALL nodes start their services.
echo "Node $(hostname): Starting worker (cores=$SPARK_WORKER_CORES memory=$SPARK_WORKER_MEMORY)..."
$SPARK_HOME/sbin/start-worker.sh "spark://$MASTER_NODE_HOSTNAME:$SPARK_MASTER_PORT"
echo "Node $(hostname): Worker registered to spark://$MASTER_NODE_HOSTNAME:$SPARK_MASTER_PORT"

# --- 5. Submit the Application (Master Node Only) ---
if [ $GLOBAL_RANK == 0 ]; then
    echo "Waiting for workers to register..."; sleep 8
    DRIVER_MEMORY="4g"
    echo "Spark submit: masterPort=${SPARK_MASTER_PORT} workers=${NUM_NODES} cores/worker=${SPARK_WORKER_CORES}"
    $SPARK_HOME/bin/spark-submit \
      --master "spark://$MASTER_NODE_HOSTNAME:$SPARK_MASTER_PORT" \
      --deploy-mode client \
      --class dm.kaist.main.MainDriver \
      --driver-memory ${DRIVER_MEMORY} \
      --conf spark.shuffle.service.enabled=false \
      /home/siepef/code/RP-DBSCAN/target/rp-dbscan-1.0-SNAPSHOT.jar \
      -i "$DATASET" -o "$OUT" -rho "$RHO" -dim "$DIM" -eps "$EPS" -minPts "$MINPTS" -np "$NUM_PARTITIONS" -M "$EXP_DIR"
    if [ $? -eq 0 ]; then echo "Success: Spark job completed."; else echo "Error: Spark job failed."; fi
fi

# --- 6. Teardown and Cleanup ---
if [ $GLOBAL_RANK == 0 ]; then
    echo "Job finished. Allowing cluster to drain before shutdown..."; sleep 15
    echo "Stopping Spark cluster (master + workers)..."
    $SPARK_HOME/sbin/stop-master.sh || true
    srun --jobid=$SLURM_JOBID $SPARK_HOME/sbin/stop-worker.sh || true
    if [ -d "$SPARK_PID_DIR" ]; then
      for f in "$SPARK_PID_DIR"/*.pid; do
        [ -f "$f" ] || continue
        pid="$(cat "$f" 2>/dev/null)"
        [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
      done
    fi
    echo "Manual cleanup (directories) will be performed by trap.";
fi
