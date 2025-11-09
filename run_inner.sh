#!/bin/bash
# Inner distributed script: executed once per Slurm task via srun from run.slurm
# Arguments: dataset dim eps minPts numPartitions expDir out rho

module purge
module load openjdk/21.0.2

DATASET=$1; DIM=$2; EPS=$3; MINPTS=$4; NUM_PARTITIONS=$5; EXP_DIR=$6; OUT=$7; RHO=$8

GLOBAL_RANK=${SLURM_PROCID:-0}
NUM_TASKS=${SLURM_NTASKS:-1}
MASTER_NODE_HOSTNAME=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -n 1)
HOST=$(hostname)
JOBID=$SLURM_JOB_ID

echo "[INNER-INIT] Host=$HOST Rank=$GLOBAL_RANK Tasks=$NUM_TASKS MasterNode=$MASTER_NODE_HOSTNAME JobID=$JOBID"

# Shared root (must be visible to all nodes). Adjust if not shared.
SCRATCH_DIR="/scratch_shared/siepef"
BAR_DIR="$SCRATCH_DIR/barrier_${JOBID}"
SPARK_CONF_DIR="$SCRATCH_DIR/spark_conf_${JOBID}"
SPARK_LOG_DIR="$SCRATCH_DIR/spark_logs_${JOBID}"
SPARK_WORKER_DIR="$SCRATCH_DIR/spark_worker_${JOBID}"
SPARK_PID_DIR="$SCRATCH_DIR/spark_pids_${JOBID}"
export SPARK_PID_DIR SPARK_CONF_DIR

# Master sets up shared dirs
if [ "$GLOBAL_RANK" -eq 0 ]; then
  mkdir -p "$SCRATCH_DIR" "$BAR_DIR" "$SPARK_CONF_DIR" "$SPARK_LOG_DIR" "$SPARK_WORKER_DIR" "$SPARK_PID_DIR" || { echo "[MASTER] ERROR creating dirs"; exit 1; }
fi

# Lightweight sync: wait for dirs to appear (simple loop)
for d in "$BAR_DIR" "$SPARK_CONF_DIR"; do
  tries=0; while [ ! -d "$d" ] && [ $tries -lt 30 ]; do sleep 1; tries=$((tries+1)); done
  [ -d "$d" ] || { echo "[RANK $GLOBAL_RANK] ERROR shared dir $d not visible"; exit 1; }
done

# Deterministic ports
BASE_PORT=$((20000 + (JOBID % 20000)))
SPARK_MASTER_PORT=$BASE_PORT
SPARK_UI_PORT=$((SPARK_MASTER_PORT + 2))
export SPARK_MASTER_PORT SPARK_UI_PORT SPARK_MASTER_HOST="$MASTER_NODE_HOSTNAME"

# Barrier helper using atomic file markers
barrier() {
  local name=$1 timeout=${2:-300}
  local path="$BAR_DIR/$name"; mkdir -p "$path" || return 1
  local marker="$path/rank_${GLOBAL_RANK}.ready"; : > "$marker"
  local start=$(date +%s)
  while true; do
    local count=$(find "$path" -maxdepth 1 -mindepth 1 -name 'rank_*' 2>/dev/null | wc -l)
    if [ "$count" -ge "$NUM_TASKS" ]; then echo "[BARRIER $name] complete $count/$NUM_TASKS"; break; fi
    local now=$(date +%s) elapsed=$((now-start))
    if [ $elapsed -ge $timeout ]; then echo "[BARRIER $name] TIMEOUT after $elapsed s"; ls -la "$path"; return 1; fi
    sleep 1
  done
}

# Master writes Spark config
if [ "$GLOBAL_RANK" -eq 0 ]; then
  cat <<EOF > "$SPARK_CONF_DIR/spark-defaults.conf"
spark.ui.port                       ${SPARK_UI_PORT}
spark.eventLog.enabled              false
spark.shuffle.service.enabled       false
EOF
  echo "[MASTER] Config written (MasterPort=$SPARK_MASTER_PORT UI=$SPARK_UI_PORT)"
fi

barrier config_ready || { echo "[ERROR] barrier config_ready failed"; exit 1; }

# Start master
if [ "$GLOBAL_RANK" -eq 0 ]; then
  echo "[MASTER] Starting master on $MASTER_NODE_HOSTNAME:$SPARK_MASTER_PORT"
  while (echo > /dev/tcp/127.0.0.1/$SPARK_MASTER_PORT) &>/dev/null; do SPARK_MASTER_PORT=$((SPARK_MASTER_PORT+1)); echo "[MASTER] Port in use, bumping to $SPARK_MASTER_PORT"; done
  export SPARK_MASTER_PORT
  $SPARK_HOME/sbin/start-master.sh --host "$MASTER_NODE_HOSTNAME" --port "$SPARK_MASTER_PORT" --webui-port "$SPARK_UI_PORT"
  while ! (echo > /dev/tcp/$MASTER_NODE_HOSTNAME/$SPARK_MASTER_PORT) &>/dev/null; do sleep 1; done
  echo "[MASTER] Master up"
fi

barrier master_ready || { echo "[ERROR] barrier master_ready failed"; exit 1; }

# Worker resources from Slurm
if [ -n "$SLURM_MEM_PER_NODE" ]; then export SPARK_WORKER_MEMORY="${SLURM_MEM_PER_NODE}m"; fi
export SPARK_WORKER_CORES=${SLURM_CPUS_PER_TASK:-$SLURM_CPUS_ON_NODE}

# Start worker on each node
echo "[WORKER] Host=$HOST Rank=$GLOBAL_RANK starting -> spark://$MASTER_NODE_HOSTNAME:$SPARK_MASTER_PORT cores=$SPARK_WORKER_CORES mem=$SPARK_WORKER_MEMORY"
$SPARK_HOME/sbin/start-worker.sh "spark://$MASTER_NODE_HOSTNAME:$SPARK_MASTER_PORT" >> "$SPARK_LOG_DIR/worker_${HOST}.log" 2>&1

barrier workers_started 180 || { echo "[ERROR] barrier workers_started failed"; exit 1; }

# Submit job
if [ "$GLOBAL_RANK" -eq 0 ]; then
  echo "[SUBMIT] Submitting application"
  DRIVER_MEMORY=4g
  $SPARK_HOME/bin/spark-submit \
    --master "spark://$MASTER_NODE_HOSTNAME:$SPARK_MASTER_PORT" \
    --deploy-mode client \
    --class dm.kaist.main.MainDriver \
    --driver-memory $DRIVER_MEMORY \
    --conf spark.shuffle.service.enabled=false \
    /home/siepef/code/RP-DBSCAN/target/rp-dbscan-1.0-SNAPSHOT.jar \
    -i "$DATASET" -o "$OUT" -rho "$RHO" -dim "$DIM" -eps "$EPS" -minPts "$MINPTS" -np "$NUM_PARTITIONS" -M "$EXP_DIR"
  SUBMIT_RC=$?
  echo "[SUBMIT] Exit code $SUBMIT_RC"
  echo "[SHUTDOWN] Stopping master & workers"
  $SPARK_HOME/sbin/stop-master.sh || true
  srun --jobid=$JOBID $SPARK_HOME/sbin/stop-worker.sh || true
fi

barrier final_cleanup 120 || echo "[WARN] final_cleanup timeout"

echo "[INNER-DONE] Rank $GLOBAL_RANK exiting"

