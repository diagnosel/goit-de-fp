"""Print actual records from the configured aggregate output topic."""
import json
from kafka import KafkaConsumer
from stream_config import kafka_options, output_topic


def main():
    consumer = KafkaConsumer(output_topic(),
                             bootstrap_servers=kafka_options()["kafka.bootstrap.servers"],
                             group_id=None, auto_offset_reset="earliest", enable_auto_commit=False,
                             consumer_timeout_ms=10000,
                             value_deserializer=lambda value: json.loads(value.decode()))
    try:
        for record in consumer:
            print(f"{record.topic}:{record.partition}:{record.offset} "
                  f"{json.dumps(record.value, ensure_ascii=False)}", flush=True)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
