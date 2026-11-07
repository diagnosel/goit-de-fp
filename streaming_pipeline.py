"""Part 1: join Kafka events to MySQL athlete data and send cumulative averages to two sinks."""
import argparse
import json
import os
import uuid
from pathlib import Path
from pyspark.sql import functions as F
from common import PROJECT, spark_session, valid_bio, join_athletes, average_stats
from stream_config import jdbc_options, kafka_options, input_topic, output_topic, output_table, identifier


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkpoint", type=Path, default=Path(os.getenv("FP_CHECKPOINT", PROJECT / ".runtime/stream_checkpoint")))
    parser.add_argument("--available-now", action="store_true", help="Finish after currently available Kafka data")
    parser.add_argument("--max-offsets-per-trigger", type=int, default=160000)
    parser.add_argument("--output-table", default=output_table())
    args = parser.parse_args()
    source_options = jdbc_options("FP_SOURCE_MYSQL")
    sink_options = jdbc_options("FP_OUTPUT_MYSQL")
    table = identifier(args.output_table)
    args.checkpoint.mkdir(parents=True, exist_ok=True)
    run_file = args.checkpoint / "pipeline_run_id.txt"
    if not run_file.exists():
        run_file.write_text(uuid.uuid4().hex)
    run_id = run_file.read_text().strip()
    spark = spark_session("FP streaming athlete averages")
    try:
        # Stage 1. Read physical measurements from the original MySQL table.
        bio = spark.read.format("jdbc").options(**source_options).option("dbtable", "athlete_bio").load()
        bio = bio.withColumn("athlete_id", F.col("athlete_id").cast("string"))
        bio_rows = bio.count()
        if bio.groupBy("athlete_id").count().filter("count > 1").limit(1).count():
            raise ValueError("Duplicate athlete_id values would multiply joined events")
        # Stage 2. Exclude missing, nonnumeric and nonfinite measurements.
        dimension = valid_bio(bio).cache()
        print(f"BIO rows={bio_rows} valid_measurements={dimension.count()}", flush=True)
        # Stage 3c. Use the real MySQL schema to parse every JSON field into its own column.
        event_schema = (spark.read.format("jdbc").options(**source_options)
                        .option("dbtable", "athlete_event_results").load().schema)
        raw = (spark.readStream.format("kafka").options(**kafka_options())
               .option("subscribe", input_topic()).option("startingOffsets", "earliest")
               .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
               .option("failOnDataLoss", "true").load())
        events = (raw.select(F.from_json(F.col("value").cast("string"), event_schema,
                                         {"mode": "FAILFAST"}).alias("event"))
                  .select("event.*").withColumn("athlete_id", F.col("athlete_id").cast("string")))
        # Stage 4. Join the streaming events to the static athlete dimension.
        joined = join_athletes(events, dimension)
        # Stage 5. Maintain cumulative means for sport, medal, sex and the bio country.
        stats = average_stats(joined)

        def write_batch(batch, batch_id):
            result = (batch.withColumn("timestamp", F.current_timestamp())
                      .withColumn("pipeline_run_id", F.lit(run_id))
                      .withColumn("batch_id", F.lit(batch_id)).cache())
            try:
                rows = result.count()
                if not rows:
                    return
                result.orderBy("sport", "medal", "sex", "country_noc").show(20, truncate=False)
                # Stage 6a. Append this complete aggregate snapshot to the Kafka output topic.
                keys = F.to_json(F.struct("pipeline_run_id", "batch_id", "sport", "medal", "sex", "country_noc"),
                                 {"ignoreNullFields": "false"})
                messages = result.select(keys.alias("key"),
                                          F.to_json(F.struct(*[F.col(c) for c in result.columns]),
                                                    {"ignoreNullFields": "false",
                                                     "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"}).alias("value"))
                messages.write.format("kafka").options(**kafka_options()).option("topic", output_topic()).save()
                # Stage 6b. Append the same materialized snapshot to the user's output table.
                result.write.format("jdbc").options(**sink_options).option("dbtable", table).mode("append").save()
                print(f"BOTH SINKS batch_id={batch_id} groups={rows}", flush=True)
            finally:
                result.unpersist()

        writer = (stats.writeStream.outputMode("complete").foreachBatch(write_batch)
                  .option("checkpointLocation", str(args.checkpoint / "spark")))
        writer = writer.trigger(availableNow=True) if args.available_now else writer.trigger(processingTime="10 seconds")
        query = writer.start()
        query.awaitTermination()
        progress_file = args.checkpoint / "progress.json"
        # Spark 4 progress includes UUID objects in its Python dictionary.
        previous = json.loads(progress_file.read_text()) if progress_file.exists() else []
        history = {item["batchId"]: item for item in previous}
        for item in query.recentProgress:
            if item["numInputRows"]:
                history[item["batchId"]] = item
        progress_file.write_text(json.dumps(list(history.values()), indent=2, default=str) + "\n")
        dimension.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
