"""Part 2 stage 3: join valid athlete measurements and write grouped averages."""
from pyspark.sql import functions as F
from common import lake_root, spark_session, valid_bio, join_athletes, average_stats, save_audit


def main():
    root = lake_root()
    spark = spark_session("FP silver to gold")
    try:
        bio = spark.read.parquet(str(root / "silver" / "athlete_bio"))
        events = spark.read.parquet(str(root / "silver" / "athlete_event_results"))
        if bio.groupBy("athlete_id").count().filter("count > 1").limit(1).count():
            raise ValueError("athlete_bio has duplicate athlete_id values; join would multiply rows")
        clean_bio = valid_bio(bio).cache()
        bio_rows, valid_rows = bio.count(), clean_bio.count()
        event_rows = events.count()
        unknown = events.join(bio.select("athlete_id"), "athlete_id", "left_anti").count()
        invalid = (events.join(bio.select("athlete_id"), "athlete_id", "inner")
                   .join(clean_bio.select("athlete_id"), "athlete_id", "left_anti").count())
        joined = join_athletes(events, clean_bio).cache()
        joined_rows = joined.count()
        stats = average_stats(joined).withColumn("timestamp", F.current_timestamp())
        stats.write.mode("errorifexists").parquet(str(root / "gold" / "avg_stats"))
        # Show the persisted result, including the same timestamp as the saved files.
        saved = spark.read.parquet(str(root / "gold" / "avg_stats"))
        print("GOLD avg_stats", flush=True)
        saved.orderBy("sport", "medal", "sex", "country_noc").show(20, truncate=False)
        saved.orderBy("sport", "medal", "sex", "country_noc").show(2, truncate=False, vertical=True)
        assert joined_rows + unknown + invalid == event_rows
        save_audit("gold", {"bio_rows": bio_rows, "valid_bio_rows": valid_rows,
                            "invalid_bio_rows": bio_rows - valid_rows,
                            "event_rows": event_rows, "unmatched_athlete_events": unknown,
                            "events_with_invalid_measurements": invalid,
                            "joined_rows": joined_rows, "groups": saved.count(),
                            "country_source": "athlete_bio.country_noc",
                            "means_weighted_by": "event-result rows, not distinct athletes",
                            "no_medal": "NULL group is kept"})
        joined.unpersist()
        clean_bio.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
