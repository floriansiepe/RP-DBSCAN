#!/bin/bash
#SBATCH --job-name=RpDBSCANJob
#SBATCH --nodes=4
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=32
#SBATCH --output=rp-dbscan_output_%j.txt
#SBATCH --time=05:00:00
#SBATCH --mail-user=siepef@uni-marburg.de
#SBATCH --mail-type=END
#SBATCH --mem=128G
# Note: Running without --exclusive. Isolation via per-job dynamic ports + dirs.

# 1. Environment (runs separately on each task now)
module purge
module load openjdk/21.0.2

# Positional args
DATASET=$1; DIM=$2; EPS=$3; MINPTS=$4; NUM_PARTITIONS=$5; EXP_DIR=$6; OUT=$7; RHO=$8

# Slurm task metadata
GLOBAL_RANK=${SLURM_PROCID:-0}
NUM_NODES=${SLURM_NTASKS:-1}
MASTER_NODE_HOSTNAME=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -n 1)
HOST=$(hostname)

echo "[INIT] Host=$HOST Rank=$GLOBAL_RANK NTasks=$NUM_NODES JobID=$SLURM_JOB_ID NodeList=$SLURM_JOB_NODELIST"

# Shared filesystem root (ensure actually shared across nodes)
SCRATCH_DIR="/scratch_shared/siepef"
TEST_FILE="$SCRATCH_DIR/.share_test_${SLURM_JOB_ID}"
if [ "$GLOBAL_RANK" -eq 0 ]; then
  mkdir -p "$SCRATCH_DIR" || { echo "[ERROR] Cannot create $SCRATCH_DIR"; exit 1; }
  echo "share-test" > "$TEST_FILE" || { echo "[ERROR] Cannot write $TEST_FILE"; exit 1; }
fi
# Small wait then verify visibility
sleep 2
if [ ! -f "$TEST_FILE" ]; then
  echo "[ERROR] Shared path $SCRATCH_DIR not visible on $HOST. Barrier cannot work. Aborting."; exit 1
fi

SPARK_CONF_DIR="$SCRATCH_DIR/spark_conf_${SLURM_JOB_ID}"
SPARK_LOG_DIR="$SCRATCH_DIR/spark_logs_${SLURM_JOB_ID}"
SPARK_WORKER_DIR="$SCRATCH_DIR/spark_worker_${SLURM_JOB_ID}"
BARRIER_DIR="$SCRATCH_DIR/barrier_${SLURM_JOB_ID}"
SPARK_PID_DIR="$SCRATCH_DIR/spark_pids_${SLURM_JOB_ID}"
export SPARK_PID_DIR SPARK_CONF_DIR

# Deterministic ports
BASE_PORT=$((20000 + (SLURM_JOB_ID % 20000)))
SPARK_MASTER_PORT=$BASE_PORT
SPARK_UI_PORT=$((SPARK_MASTER_PORT + 2))
export SPARK_MASTER_PORT SPARK_UI_PORT SPARK_MASTER_HOST="$MASTER_NODE_HOSTNAME"

# Cleanup (only master rank performs directory removal)
cleanup() {
  if [ "$GLOBAL_RANK" -eq 0 ]; then
    echo "[CLEANUP] Stopping Spark cluster for job $SLURM_JOB_ID";
    if [ -n "$SPARK_HOME" ]; then
      $SPARK_HOME/sbin/stop-master.sh || true
      srun --jobid=$SLURM_JOBID $SPARK_HOME/sbin/stop-worker.sh || true
    fi
    rm -rf "$SPARK_CONF_DIR" "$SPARK_LOG_DIR" "$SPARK_WORKER_DIR" "$BARRIER_DIR" "$SPARK_PID_DIR" 2>/dev/null || true
  fi
}
trap cleanup EXIT

# Resource hints for worker daemons (Spark will ingest these env vars)
if [ -n "$SLURM_MEM_PER_NODE" ]; then export SPARK_WORKER_MEMORY="${SLURM_MEM_PER_NODE}m"; fi
export SPARK_WORKER_CORES=${SLURM_CPUS_PER_TASK:-$SLURM_CPUS_ON_NODE}

# Barrier primitive
barrier_sync() {
  local name=$1 timeout=${2:-300}
  local path="$BARRIER_DIR/$name"
  mkdir -p "$path" || { echo "[BAR:$name] ERROR cannot mkdir $path"; return 1; }
  local marker="$path/node_${GLOBAL_RANK}.ready"
  : > "$marker" || { echo "[BAR:$name] ERROR cannot create marker $marker"; return 1; }
  echo "[BAR:$name] Rank $GLOBAL_RANK waiting for $NUM_NODES markers (timeout=$timeout)s";
  local start; start=$(date +%s)
  local prev=-1
  while true; do
    local count=0
    [ -d "$path" ] && count=$(find "$path" -maxdepth 1 -mindepth 1 -name 'node_*' | wc -l || true)
    if [ "$count" -ge "$NUM_NODES" ]; then
      echo "[BAR:$name] All $count markers present"; break
    fi
    if [ "$count" -ne "$prev" ]; then echo "[BAR:$name] Progress $count/$NUM_NODES"; prev=$count; fi
    local now; now=$(date +%s)
    local elapsed=$((now-start))
    if [ $elapsed -ge $timeout ]; then
      echo "[BAR:$name] TIMEOUT after $elapsed s"; ls -la "$path" || true; return 1
    fi
    if [ $((elapsed % 30)) -eq 0 ]; then echo "[BAR:$name] Debug listing:"; ls -1 "$path" || true; fi
    sleep 1
  done
  return 0
}

# Master creates configs
if [ "$GLOBAL_RANK" -eq 0 ]; then
  echo "[MASTER] Preparing Spark conf/log/worker dirs"
  mkdir -p "$SPARK_CONF_DIR" "$SPARK_LOG_DIR" "$SPARK_WORKER_DIR" "$BARRIER_DIR" "$SPARK_PID_DIR"
  cat <<EOF > "$SPARK_CONF_DIR/spark-defaults.conf"
spark.ui.port                       ${SPARK_UI_PORT}
spark.eventLog.enabled              false
spark.shuffle.service.enabled       false
EOF
  echo "[MASTER] spark-defaults.conf written (MasterPort=$SPARK_MASTER_PORT UI=$SPARK_UI_PORT)"
fi

barrier_sync config_ready || { echo "[ERROR] Barrier config_ready failed"; exit 1; }

# Start master
if [ "$GLOBAL_RANK" -eq 0 ]; then
  echo "[MASTER] Starting Spark Master on $MASTER_NODE_HOSTNAME:$SPARK_MASTER_PORT"
  # Port collision check
  while (echo > /dev/tcp/127.0.0.1/$SPARK_MASTER_PORT) &>/dev/null; do
    SPARK_MASTER_PORT=$((SPARK_MASTER_PORT+1))
    echo "[MASTER] Port busy, trying $SPARK_MASTER_PORT"
  done
  export SPARK_MASTER_PORT
  $SPARK_HOME/sbin/start-master.sh --host "$MASTER_NODE_HOSTNAME" --port "$SPARK_MASTER_PORT" --webui-port "$SPARK_UI_PORT"
  echo "[MASTER] Waiting for master port..."
  while ! (echo > /dev/tcp/$MASTER_NODE_HOSTNAME/$SPARK_MASTER_PORT) &>/dev/null; do sleep 1; done
  echo "[MASTER] Master up"
fi

barrier_sync master_ready || { echo "[ERROR] Barrier master_ready failed"; exit 1; }

# Start worker on each task host
echo "[WORKER] Host=$HOST Rank=$GLOBAL_RANK starting worker -> spark://$MASTER_NODE_HOSTNAME:$SPARK_MASTER_PORT (cores=$SPARK_WORKER_CORES mem=$SPARK_WORKER_MEMORY)"
$SPARK_HOME/sbin/start-worker.sh "spark://$MASTER_NODE_HOSTNAME:$SPARK_MASTER_PORT" >> "$SPARK_LOG_DIR/worker_${HOST}.log" 2>&1

# Only master submits job
if [ "$GLOBAL_RANK" -eq 0 ]; then
  echo "[SUBMIT] Sleeping to allow workers to register"; sleep 8
  DRIVER_MEMORY=4g
  $SPARK_HOME/bin/spark-submit \
    --master "spark://$MASTER_NODE_HOSTNAME:$SPARK_MASTER_PORT" \
    --deploy-mode client \
    --class dm.kaist.main.MainDriver \
    --driver-memory $DRIVER_MEMORY \
    --conf spark.shuffle.service.enabled=false \
    /home/siepef/code/RP-DBSCAN/target/rp-dbscan-1.0-SNAPSHOT.jar \
    -i "$DATASET" -o "$OUT" -rho "$RHO" -dim "$DIM" -eps "$EPS" -minPts "$MINPTS" -np "$NUM_PARTITIONS" -M "$EXP_DIR"
  rc=$?
  echo "[SUBMIT] spark-submit exit code=$rc"; [ $rc -ne 0 ] && echo "[SUBMIT] ERROR job failed"
  echo "[SUBMIT] Allowing drain"; sleep 15
  echo "[SUBMIT] Initiating cluster shutdown"
  $SPARK_HOME/sbin/stop-master.sh || true
  srun --jobid=$SLURM_JOBID $SPARK_HOME/sbin/stop-worker.sh || true
fi

barrier_sync final_cleanup 60 || echo "[WARN] final_cleanup barrier not reached fully"

# End
echo "[DONE] Rank $GLOBAL_RANK exiting"
