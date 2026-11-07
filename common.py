"""Shared Spark setup and explicit cleaning rules."""
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType

PROJECT = Path(__file__).resolve().parent
TABLES = ("athlete_bio", "athlete_event_results")
SOURCES = {table: f"https://ftp.goit.study/neoversity/{table}.csv" for table in TABLES}


def lake_root():
    return Path(os.getenv("FP_LAKE_ROOT", str(PROJECT / "lake"))).resolve()


def spark_session(name):
    spark = (SparkSession.builder.master("local[2]").appName(name)
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    print(f"EXECUTION TIME UTC {datetime.now(timezone.utc).isoformat()}", flush=True)
    return spark


def clean_text(df):
    # Normalize whitespace only. Keep names, Unicode letters and punctuation.
    return df.select(*[
        F.trim(F.regexp_replace(F.col(field.name), r"[\s\u00A0]+", " ")).alias(field.name)
        if isinstance(field.dataType, StringType) else F.col(field.name)
        for field in df.schema.fields])


def valid_bio(df):
    # Raw CSV measurements can end with NBSP. Remove edge whitespace only.
    edged = df
    for name in ("height", "weight"):
        edged = edged.withColumn(name, F.regexp_replace(F.col(name).cast("string"),
                                                       r"^[\s\u00A0]+|[\s\u00A0]+$", ""))
    parsed = (edged.withColumn("height", F.expr("try_cast(height as double)"))
              .withColumn("weight", F.expr("try_cast(weight as double)")))
    finite = lambda name: (F.col(name).isNotNull() & ~F.isnan(name)
                           & (F.abs(F.col(name)) < F.lit(float("inf"))))
    return parsed.filter(finite("height") & finite("weight"))


def join_athletes(events, bio):
    # country_noc means the athlete's bio country, not the event delegation.
    return (events.alias("event").join(bio.alias("bio"), "athlete_id", "inner")
            .select(F.col("event.sport").alias("sport"),
                    F.when(F.trim(F.col("event.medal")) == "", F.lit(None))
                     .otherwise(F.col("event.medal")).alias("medal"),
                    F.col("bio.sex").alias("sex"),
                    F.col("bio.country_noc").alias("country_noc"),
                    F.col("bio.height").alias("height"),
                    F.col("bio.weight").alias("weight")))


def average_stats(joined):
    return (joined.groupBy("sport", "medal", "sex", "country_noc")
            .agg(F.avg("height").alias("avg_height"),
                 F.avg("weight").alias("avg_weight"),
                 F.count(F.lit(1)).alias("event_count")))


def save_audit(stage, values):
    target = lake_root() / "audit"
    target.mkdir(parents=True, exist_ok=True)
    (target / f"{stage}.json").write_text(json.dumps(values, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps(values, ensure_ascii=False), flush=True)
