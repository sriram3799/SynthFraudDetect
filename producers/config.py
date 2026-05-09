"""
Producer configuration dataclass.
All values are supplied via CLI args or environment variables — never hardcoded.
"""

import os
from dataclasses import dataclass, field


@dataclass
class ProducerConfig:
    bootstrap_servers: str = field(
        default_factory=lambda: os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    topic_name: str = "transactions"
    target_tps: int = 25_000
    fraud_injection_rate: float = 0.02   # 2% of total volume
    seed: int = 42
    duration_seconds: int = 0            # 0 = run forever
    num_workers: int = 0                 # 0 = one per CPU core
    batch_size: int = 65_536             # Kafka producer batch.size bytes
    linger_ms: int = 5
    partitions: int = 12   # matches create-topics.sh — 2K tps/partition at 25K target
