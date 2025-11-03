#!/bin/bash
#SBATCH --job-name=MrDBSCANJob
#SBATCH --nodes=4
#SBATCH --tasks-per-node=1
#SBATCH --cpus-per-task=32
#SBATCH --output=mr-dbscan_output_%j.txt
#SBATCH --time=01:00:00
#SBATCH --mail-user=siepef@uni-marburg.de
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

# Define paths on the SHARED filesystem
SCRATCH_DIR="/scratch_shared/siepef"
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
    local barrier_path="$BARRIER_DIR/$barrier_name"
    mkdir -p "$barrier_path"

    # This node signals it has reached the barrier
    touch "$barrier_path/node_${GLOBAL_RANK}_ready"

    echo "Node $(hostname): Reached barrier '$barrier_name'. Waiting for other nodes..."

    # Wait until all nodes have signaled
    while [ $(ls -1 "$barrier_path" | wc -l) -lt $NUM_NODES ]; do
        sleep 1
    done

    echo "Node $(hostname): Passed barrier '$barrier_name'."
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
      home/siepef/code/RP-DBSCAN/target/rp-dbscan-1.0-SNAPSHOT.jar \
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
fi
