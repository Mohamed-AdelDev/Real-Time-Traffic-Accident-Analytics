from pathlib import Path

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "processed-accidents"

# Spark configuration
APP_NAME = "RealTimeTrafficAccidentAnalytics"
SPARK_MASTER = "local[2]"

# Streaming configuration
STARTING_OFFSETS = "latest"
MAX_OFFSETS_PER_TRIGGER = 100
TRIGGER_INTERVAL = "10 seconds"

# Checkpoint configuration
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CHECKPOINT_LOCATION = str(
    PROJECT_ROOT / "checkpoints" / "spark_streaming"
)
