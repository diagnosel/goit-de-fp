"""Part 2 stage 1: keep original CSV files and write bronze Parquet."""
import hashlib
import subprocess
from pathlib import Path
from common import TABLES, SOURCES, lake_root, spark_session, save_audit


def main():
    root = lake_root()
    (root / "landing").mkdir(parents=True, exist_ok=True)
    spark = spark_session("FP landing to bronze")
    audit = {}
    try:
        for table in TABLES:
            source = root / "landing" / f"{table}.csv"
            if not source.exists():
                temporary = source.with_suffix(".csv.part")
                subprocess.run(["curl", "-fL", "--retry", "2", "--connect-timeout", "20",
                                SOURCES[table], "-o", str(temporary)], check=True)
                temporary.replace(source)
            df = (spark.read.option("header", True).option("inferSchema", False)
                  .option("multiLine", True).option("escape", '"')
                  .option("mode", "FAILFAST").csv(str(source)))
            rows = df.count()
            df.write.mode("errorifexists").parquet(str(root / "bronze" / table))
            print(f"BRONZE {table} rows={rows}", flush=True)
            df.show(5, truncate=55)
            df.show(1, truncate=50, vertical=True)
            audit[table] = {"source_url": SOURCES[table], "rows": rows,
                            "sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
                            "columns": df.columns, "all_columns_read_as_text": True}
        save_audit("bronze", audit)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
