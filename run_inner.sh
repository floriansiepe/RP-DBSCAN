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

SCRATCH_DIR="/scratch_shared/siepef"
BAR_DIR="$SCRATCH_DIR/barrier_${JOBID}"
SPARK_CONF_DIR="$SCRATCH_DIR/spark_conf_${JOBID}"
SPARK_LOG_DIR="$SCRATCH_DIR/spark_logs_${JOBID}"
SPARK_WORKER_DIR="$SCRATCH_DIR/spark_worker_${JOBID}"
SPARK_PID_DIR="$SCRATCH_DIR/spark_pids_${JOBID}"
export SPARK_PID_DIR SPARK_CONF_DIR

if [ "$GLOBAL_RANK" -eq 0 ]; then
  mkdir -p "$SCRATCH_DIR" "$BAR_DIR" "$SPARK_CONF_DIR" "$SPARK_LOG_DIR" "$SPARK_WORKER_DIR" "$SPARK_PID_DIR" || { echo "[MASTER] ERROR creating dirs"; exit 1; }
fi

for d in "$BAR_DIR" "$SPARK_CONF_DIR"; do
  tries=0; while [ ! -d "$d" ] && [ $tries -lt 30 ]; do sleep 1; tries=$((tries+1)); done
  [ -d "$d" ] || { echo "[RANK $GLOBAL_RANK] ERROR shared dir $d not visible"; exit 1; }
done

BASE_PORT=$((20000 + (JOBID % 20000)))
SPARK_MASTER_PORT=$BASE_PORT
SPARK_UI_PORT=$((SPARK_MASTER_PORT + 2))
export SPARK_MASTER_PORT SPARK_UI_PORT SPARK_MASTER_HOST="$MASTER_NODE_HOSTNAME"

barrier() {
  local name=$1 timeout=${2:-300}
  local path="$BAR_DIR/$name"; mkdir -p "$path" || return 1
  local marker="$path/rank_${GLOBAL_RANK}.ready"; : > "$marker"
  local start; start=$(date +%s)
  while true; do
    local count; count=$(find "$path" -maxdepth 1 -mindepth 1 -name 'rank_*' 2>/dev/null | wc -l)
    if [ "$count" -ge "$NUM_TASKS" ]; then echo "[BARRIER $name] complete $count/$NUM_TASKS"; break; fi
    local now; now=$(date +%s); local elapsed=$((now-start))
    if [ $elapsed -ge $timeout ]; then echo "[BARRIER $name] TIMEOUT after $elapsed s"; ls -la "$path"; return 1; fi
    sleep 1
  done
}

if [ "$GLOBAL_RANK" -eq 0 ]; then
  cat <<EOF > "$SPARK_CONF_DIR/spark-defaults.conf"
spark.ui.port                       ${SPARK_UI_PORT}
spark.eventLog.enabled              false
spark.shuffle.service.enabled       false
spark.serializer                    org.apache.spark.serializer.KryoSerializer
spark.kryo.unsafe                   true
EOF
  echo "[MASTER] Config written (MasterPort=$SPARK_MASTER_PORT UI=$SPARK_UI_PORT)"
fi

barrier config_ready || { echo "[ERROR] barrier config_ready failed"; exit 1; }

if [ "$GLOBAL_RANK" -eq 0 ]; then
  echo "[MASTER] Starting master on $MASTER_NODE_HOSTNAME:$SPARK_MASTER_PORT"
  while (echo > /dev/tcp/127.0.0.1/$SPARK_MASTER_PORT) &>/dev/null; do SPARK_MASTER_PORT=$((SPARK_MASTER_PORT+1)); echo "[MASTER] Port in use, bumping to $SPARK_MASTER_PORT"; done
  export SPARK_MASTER_PORT
  $SPARK_HOME/sbin/start-master.sh --host "$MASTER_NODE_HOSTNAME" --port "$SPARK_MASTER_PORT" --webui-port "$SPARK_UI_PORT"
  while ! (echo > /dev/tcp/$MASTER_NODE_HOSTNAME/$SPARK_MASTER_PORT) &>/dev/null; do sleep 1; done
  echo "[MASTER] Master up"
fi

barrier master_ready || { echo "[ERROR] barrier master_ready failed"; exit 1; }

if [ -n "$SLURM_MEM_PER_NODE" ]; then export SPARK_WORKER_MEMORY="${SLURM_MEM_PER_NODE}m"; fi
export SPARK_WORKER_CORES=${SLURM_CPUS_PER_TASK:-$SLURM_CPUS_ON_NODE}

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
    echo "[TUNING] AUTO_EXECUTOR_SPLIT enabled -> executorsPerWorker=$EXECUTORS_PER_WORKER totalInstances=$SPARK_EXECUTOR_INSTANCES coresPerExec=$SPARK_EXECUTOR_CORES memPerExec=$SPARK_EXECUTOR_MEMORY"
  fi
fi

if [ -z "$SPARK_DEFAULT_PARALLELISM" ]; then
  if [ -n "$SPARK_EXECUTOR_INSTANCES" ]; then
    TOTAL_CORES=$(( SPARK_EXECUTOR_CORES * SPARK_EXECUTOR_INSTANCES ))
  else
    TOTAL_CORES=$(( SPARK_EXECUTOR_CORES * NUM_TASKS ))
  fi
  SPARK_DEFAULT_PARALLELISM=$(( TOTAL_CORES * 2 ))
fi

echo "[WORKER] Host=$HOST Rank=$GLOBAL_RANK starting worker -> spark://$MASTER_NODE_HOSTNAME:$SPARK_MASTER_PORT cores=$SPARK_WORKER_CORES mem=$SPARK_WORKER_MEMORY"
$SPARK_HOME/sbin/start-worker.sh "spark://$MASTER_NODE_HOSTNAME:$SPARK_MASTER_PORT" >> "$SPARK_LOG_DIR/worker_${HOST}.log" 2>&1

barrier workers_started 180 || { echo "[ERROR] barrier workers_started failed"; exit 1; }

SUBMIT_RC=0
if [ "$GLOBAL_RANK" -eq 0 ]; then
  echo "[SUBMIT] Launching spark-submit"
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
    --conf spark.executor.memoryOverhead=4096 \
    --conf spark.memory.fraction=0.60 \
    --conf spark.memory.storageFraction=0.30 \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.kryo.unsafe=true \
    --conf spark.executor.extraJavaOptions="-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35" \
    --conf spark.driver.extraJavaOptions="-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35" \
    /home/siepef/code/RP-DBSCAN/target/rp-dbscan-1.0-SNAPSHOT.jar \
    -i "$DATASET" -o "$OUT" -rho "$RHO" -dim "$DIM" -eps "$EPS" -minPts "$MINPTS" -np "$NUM_PARTITIONS" -M "$EXP_DIR"
  SUBMIT_RC=$?
  echo "$SUBMIT_RC" > "$BAR_DIR/submit_rc"
  echo "[SUBMIT] Exit code $SUBMIT_RC"
fi

# Barrier so all ranks wait until submit_rc exists
barrier submit_done 600 || { echo "[ERROR] submit_done barrier failed"; exit 1; }

# Every rank loads the submit_rc and proceeds to shutdown local worker
if [ "$GLOBAL_RANK" -ne 0 ]; then
  if [ -f "$BAR_DIR/submit_rc" ]; then SUBMIT_RC=$(cat "$BAR_DIR/submit_rc"); else SUBMIT_RC=1; fi
fi

echo "[SHUTDOWN] Rank $GLOBAL_RANK stopping local worker"
$SPARK_HOME/sbin/stop-worker.sh || echo "[SHUTDOWN] Rank $GLOBAL_RANK worker stop returned non-zero"

# Master stops master after workers have initiated stop
barrier workers_stopping 60 || echo "[WARN] workers_stopping barrier timeout"
if [ "$GLOBAL_RANK" -eq 0 ]; then
  echo "[SHUTDOWN] Master stopping master daemon"
  $SPARK_HOME/sbin/stop-master.sh || echo "[SHUTDOWN] Master stop-master returned non-zero"
fi

barrier master_stopped 60 || echo "[WARN] master_stopped barrier timeout"

if [ "$SUBMIT_RC" -ne 0 ]; then
  echo "[INNER-DONE] Rank $GLOBAL_RANK exiting with FAILURE rc=$SUBMIT_RC"
else
  echo "[INNER-DONE] Rank $GLOBAL_RANK exiting with SUCCESS"
fi
exit $SUBMIT_RC
