"""Part 1 stage 3a: read the course MySQL results table and publish JSON to Kafka."""
from pyspark.sql import functions as F
from common import spark_session
from stream_config import jdbc_options, kafka_options, input_topic


def main():
    options = jdbc_options("FP_SOURCE_MYSQL")
    spark = spark_session("FP MySQL events to Kafka")
    try:
        # Stage 3a. Read the original MySQL table with Spark JDBC.
        events = spark.read.format("jdbc").options(**options).option("dbtable", "athlete_event_results").load()
        if not {"athlete_id", "sport", "medal"}.issubset(events.columns):
            raise ValueError("The source event table is missing required fields")
        print(f"SOURCE event rows={events.count()}", flush=True)
        events.printSchema()
        # Stage 3b. JSON contains every source column, including explicit null values.
        records = events.select(F.col("athlete_id").cast("string").alias("key"),
                                F.to_json(F.struct(*[F.col(c) for c in events.columns]),
                                          {"ignoreNullFields": "false"}).alias("value"))
        records.write.format("kafka").options(**kafka_options()).option("topic", input_topic()).save()
        print("Source events published to the configured Kafka input topic", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
