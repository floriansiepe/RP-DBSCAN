#!/bin/bash
# Inner distributed script: executed once per Slurm task via srun from run.slurm
# Arguments: dataset dim eps minPts numPartitions expDir out rho
# Robust against non-shared filesystem: no cross-node directory synchronization required.

module purge
module load openjdk/21.0.2

DATASET=$1; DIM=$2; EPS=$3; MINPTS=$4; NUM_PARTITIONS=$5; EXP_DIR=$6; OUT=$7; RHO=$8

GLOBAL_RANK=${SLURM_PROCID:-0}
NUM_TASKS=${SLURM_NTASKS:-1}
MASTER_NODE_HOSTNAME=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -n 1)
HOST=$(hostname)
JOBID=$SLURM_JOB_ID

echo "[INNER-INIT] Host=$HOST Rank=$GLOBAL_RANK Tasks=$NUM_TASKS MasterNode=$MASTER_NODE_HOSTNAME JobID=$JOBID"

# Deterministic ports (no shared file needed)
BASE_PORT=$((20000 + (JOBID % 20000)))
SPARK_MASTER_PORT=$BASE_PORT
SPARK_UI_PORT=$((SPARK_MASTER_PORT + 2))
export SPARK_MASTER_PORT SPARK_UI_PORT SPARK_MASTER_HOST="$MASTER_NODE_HOSTNAME"

# Memory / cores from Slurm
if [ -n "$SLURM_MEM_PER_NODE" ]; then export SPARK_WORKER_MEMORY="${SLURM_MEM_PER_NODE}m"; fi
export SPARK_WORKER_CORES=${SLURM_CPUS_PER_TASK:-$SLURM_CPUS_ON_NODE}

# Executor sizing
if [ -n "$SLURM_MEM_PER_NODE" ]; then
  TOTAL_MEM_MB=$SLURM_MEM_PER_NODE
  EXECUTOR_MEM_MB=$(( TOTAL_MEM_MB * 80 / 100 ))
  [ $EXECUTOR_MEM_MB -lt 1024 ] && EXECUTOR_MEM_MB=1024
  DEFAULT_EXECUTOR_MEMORY="${EXECUTOR_MEM_MB}m"
else
  DEFAULT_EXECUTOR_MEMORY="8g"
fi
SPARK_EXECUTOR_MEMORY=${SPARK_EXECUTOR_MEMORY:-$DEFAULT_EXECUTOR_MEMORY}
SPARK_EXECUTOR_CORES=${SPARK_EXECUTOR_CORES:-$SPARK_WORKER_CORES}
DRIVER_MEMORY=${DRIVER_MEMORY:-8g}

# Optional splitting
if [ "${AUTO_EXECUTOR_SPLIT:-0}" = "1" ]; then
  TARGET_CORES_PER_EXEC=${TARGET_CORES_PER_EXEC:-8}
  if [ $TARGET_CORES_PER_EXEC -lt $SPARK_WORKER_CORES ]; then
    SPARK_EXECUTOR_CORES=$TARGET_CORES_PER_EXEC
    EXECUTORS_PER_WORKER=$(( SPARK_WORKER_CORES / SPARK_EXECUTOR_CORES ))
    [ $EXECUTORS_PER_WORKER -lt 1 ] && EXECUTORS_PER_WORKER=1
    if [ -n "$SLURM_MEM_PER_NODE" ]; then
      PER_EXEC_MB=$(( (SLURM_MEM_PER_NODE * 80 / 100) / EXECUTORS_PER_WORKER ))
      [ $PER_EXEC_MB -lt 1024 ] && PER_EXEC_MB=1024
      SPARK_EXECUTOR_MEMORY="${PER_EXEC_MB}m"
    fi
    SPARK_EXECUTOR_INSTANCES=$(( EXECUTORS_PER_WORKER * NUM_TASKS ))
    echo "[TUNING] Executor split enabled executorsPerWorker=$EXECUTORS_PER_WORKER instances=$SPARK_EXECUTOR_INSTANCES coresPerExec=$SPARK_EXECUTOR_CORES memPerExec=$SPARK_EXECUTOR_MEMORY"
  fi
fi

if [ -z "$SPARK_DEFAULT_PARALLELISM" ]; then
  if [ -n "$SPARK_EXECUTOR_INSTANCES" ]; then TOTAL_CORES=$(( SPARK_EXECUTOR_CORES * SPARK_EXECUTOR_INSTANCES )); else TOTAL_CORES=$(( SPARK_EXECUTOR_CORES * NUM_TASKS )); fi
  SPARK_DEFAULT_PARALLELISM=$(( TOTAL_CORES * 2 ))
fi

# Function: wait for master port open
wait_for_master() {
  local timeout=${1:-300} start end
  start=$(date +%s)
  while true; do
    (echo > /dev/tcp/$MASTER_NODE_HOSTNAME/$SPARK_MASTER_PORT) &>/dev/null && return 0
    end=$(date +%s); if [ $((end-start)) -ge $timeout ]; then echo "[WAIT] Timeout waiting for master $MASTER_NODE_HOSTNAME:$SPARK_MASTER_PORT"; return 1; fi
    sleep 1
  done
}

# Rank 0: start master
if [ "$GLOBAL_RANK" -eq 0 ]; then
  echo "[MASTER] Starting master host=$MASTER_NODE_HOSTNAME port=$SPARK_MASTER_PORT ui=$SPARK_UI_PORT"
  while (echo > /dev/tcp/127.0.0.1/$SPARK_MASTER_PORT) &>/dev/null; do SPARK_MASTER_PORT=$((SPARK_MASTER_PORT+1)); echo "[MASTER] Port in use bump -> $SPARK_MASTER_PORT"; done
  export SPARK_MASTER_PORT
  $SPARK_HOME/sbin/start-master.sh --host "$MASTER_NODE_HOSTNAME" --port "$SPARK_MASTER_PORT" --webui-port "$SPARK_UI_PORT"
  wait_for_master 120 || { echo "[MASTER] Failed to detect master port open"; exit 1; }
  echo "[MASTER] Master up on spark://$MASTER_NODE_HOSTNAME:$SPARK_MASTER_PORT"
fi

# Non-master ranks wait for master
if [ "$GLOBAL_RANK" -ne 0 ]; then
  echo "[WORKER-$GLOBAL_RANK] Waiting for master spark://$MASTER_NODE_HOSTNAME:$SPARK_MASTER_PORT"
  wait_for_master 600 || { echo "[WORKER-$GLOBAL_RANK] Master not reachable"; exit 1; }
fi

# Start worker on all ranks (including master if desired). We keep symmetrical startup.
echo "[WORKER-$GLOBAL_RANK] Starting worker cores=$SPARK_WORKER_CORES mem=$SPARK_WORKER_MEMORY -> spark://$MASTER_NODE_HOSTNAME:$SPARK_MASTER_PORT"
$SPARK_HOME/sbin/start-worker.sh "spark://$MASTER_NODE_HOSTNAME:$SPARK_MASTER_PORT" || { echo "[WORKER-$GLOBAL_RANK] Failed to start worker"; exit 1; }

# Rank 0 submits job
SUBMIT_RC=0
if [ "$GLOBAL_RANK" -eq 0 ]; then
  echo "[SUBMIT] spark-submit (executorMemory=$SPARK_EXECUTOR_MEMORY executorCores=$SPARK_EXECUTOR_CORES parallelism=$SPARK_DEFAULT_PARALLELISM instances=${SPARK_EXECUTOR_INSTANCES:-1})"
  $SPARK_HOME/bin/spark-submit \
    --master "spark://$MASTER_NODE_HOSTNAME:$SPARK_MASTER_PORT" \
    --deploy-mode client \
    --class dm.kaist.main.MainDriver \
    --driver-memory $DRIVER_MEMORY \
    --executor-memory $SPARK_EXECUTOR_MEMORY \
    --conf spark.executor.cores=$SPARK_EXECUTOR_CORES \
    ${SPARK_EXECUTOR_INSTANCES:+--conf spark.executor.instances=$SPARK_EXECUTOR_INSTANCES} \
    --conf spark.default.parallelism=$SPARK_DEFAULT_PARALLELISM \
    --conf spark.shuffle.service.enabled=false \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.kryo.unsafe=true \
    --conf spark.executor.memoryOverhead=4096 \
    --conf spark.memory.fraction=0.60 \
    --conf spark.memory.storageFraction=0.30 \
    --conf spark.executor.extraJavaOptions="-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35" \
    --conf spark.driver.extraJavaOptions="-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35" \
    /home/siepef/code/RP-DBSCAN/target/rp-dbscan-1.0-SNAPSHOT.jar \
    -i "$DATASET" -o "$OUT" -rho "$RHO" -dim "$DIM" -eps "$EPS" -minPts "$MINPTS" -np "$NUM_PARTITIONS" -M "$EXP_DIR"
  SUBMIT_RC=$?
  echo "[SUBMIT] Exit code $SUBMIT_RC"
  echo "[MASTER] Stopping master"
  $SPARK_HOME/sbin/stop-master.sh || echo "[MASTER] stop-master returned non-zero"
fi

# Workers (including rank0 after submit) wait for master port to close then stop own worker.
if [ "$GLOBAL_RANK" -ne 0 ]; then
  echo "[WORKER-$GLOBAL_RANK] Monitoring master port for shutdown"
  while (echo > /dev/tcp/$MASTER_NODE_HOSTNAME/$SPARK_MASTER_PORT) &>/dev/null; do sleep 2; done
  echo "[WORKER-$GLOBAL_RANK] Detected master down"
fi

# Stop worker
echo "[WORKER-$GLOBAL_RANK] Stopping worker"
$SPARK_HOME/sbin/stop-worker.sh || echo "[WORKER-$GLOBAL_RANK] stop-worker returned non-zero"

# Final safety cleanup for stray daemons
ps -u "$USER" -o pid,cmd | awk '/org.apache.spark.deploy.(worker.Worker|master.Master)/ {print $1}' | while read -r spid; do kill "$spid" 2>/dev/null || true; done

# Non-master ranks infer success from master shutdown if SUBMIT_RC not propagated; propagate via environment if available
if [ "$GLOBAL_RANK" -ne 0 ]; then
  # We cannot read master's rc directly without shared FS; assume success unless stray Spark processes remain.
  REMAINING=$(ps -u "$USER" -o cmd | grep -c 'org.apache.spark.deploy.worker.Worker' || true)
  if [ "$REMAINING" -gt 0 ]; then SUBMIT_RC=1; fi
fi

if [ "$SUBMIT_RC" -ne 0 ]; then
  echo "[INNER-DONE] Rank $GLOBAL_RANK exiting with FAILURE rc=$SUBMIT_RC"
else
  echo "[INNER-DONE] Rank $GLOBAL_RANK exiting with SUCCESS"
fi
exit $SUBMIT_RC
