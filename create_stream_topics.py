"""Create only the two isolated final-project Kafka topics."""
from kafka.admin import KafkaAdminClient, NewTopic
from stream_config import kafka_options, input_topic, output_topic


def main():
    broker = kafka_options()["kafka.bootstrap.servers"]
    if any(address.split(":")[0] not in {"127.0.0.1", "localhost"} for address in broker.split(",")):
        raise ValueError("This helper creates topics only on a local teaching broker")
    admin = KafkaAdminClient(bootstrap_servers=broker)
    try:
        existing = set(admin.list_topics())
        new = [NewTopic(name, num_partitions=1, replication_factor=1)
               for name in (input_topic(), output_topic()) if name not in existing]
        if new:
            admin.create_topics(new_topics=new)
        print("\n".join(name for name in sorted(admin.list_topics())
                        if name in {input_topic(), output_topic()}))
    finally:
        admin.close()


if __name__ == "__main__":
    main()
