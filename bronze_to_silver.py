"""Part 2 stage 2: clean every text column and remove exact duplicate rows."""
from common import TABLES, lake_root, spark_session, clean_text, save_audit


def main():
    root = lake_root()
    spark = spark_session("FP bronze to silver")
    audit = {}
    try:
        for table in TABLES:
            bronze = spark.read.parquet(str(root / "bronze" / table))
            before = bronze.count()
            silver = clean_text(bronze).dropDuplicates().cache()
            after = silver.count()
            silver.write.mode("errorifexists").parquet(str(root / "silver" / table))
            print(f"SILVER {table} before={before} after={after}", flush=True)
            silver.orderBy("athlete_id").show(5, truncate=55)
            silver.orderBy("athlete_id").show(1, truncate=50, vertical=True)
            audit[table] = {"before": before, "after": after,
                            "duplicates_removed_after_text_cleaning": before - after,
                            "text_rule": "trim and collapse whitespace; keep Unicode and punctuation"}
            silver.unpersist()
        save_audit("silver", audit)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
