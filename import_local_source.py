"""Import both official CSV files into new tables on a private loopback MySQL."""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import mysql.connector
from common import PROJECT, TABLES, SOURCES
from stream_config import identifier

HASHES = {
    "athlete_bio": "63781c43de4b7376f690b96e86063854034b2e4477a7e16e617a71c26623d356",
    "athlete_event_results": "4a76c66fc6f69a2f69b4a953298d475fd61fb46a40ac5995ea738bc94695905b",
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv-dir", type=Path, default=Path(os.getenv("FP_SOURCE_CSV_DIR", PROJECT / ".runtime/landing")))
    args = parser.parse_args()
    host = os.getenv("FP_SOURCE_MYSQL_HOST", "127.0.0.1")
    if host not in {"127.0.0.1", "localhost", "::1"}:
        raise ValueError("Import is allowed only on your own loopback MySQL")
    files = {}
    for table in TABLES:
        path = args.csv_dir / f"{table}.csv"
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != HASHES[table]:
            raise ValueError(f"The official CSV checksum does not match: {table}")
        files[table] = path
    connection = mysql.connector.connect(host=host,
        port=int(os.getenv("FP_SOURCE_MYSQL_PORT", "13310")),
        user=os.environ["FP_IMPORT_MYSQL_USER"],
        password=os.environ.get("FP_IMPORT_MYSQL_PASSWORD", ""))
    audit = {"source": "Official course CSV imported into a private local MySQL",
             "remote_course_mysql_used": False, "tables": {}}
    try:
        cursor = connection.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS olympic_dataset CHARACTER SET utf8mb4")
        cursor.execute("USE olympic_dataset")
        # Check both tables before creating either. Never replace an existing source.
        for table in TABLES:
            cursor.execute("SHOW TABLES LIKE %s", (table,))
            if cursor.fetchone():
                raise ValueError(f"Source table already exists: {table}. Use a new local server.")
        for table, path in files.items():
            with path.open(encoding="utf-8-sig", newline="") as source:
                reader = csv.DictReader(source)
                columns = [identifier(name) for name in reader.fieldnames]
                definitions = ", ".join(f"`{name}` LONGTEXT" for name in columns)
                cursor.execute(f"CREATE TABLE `{table}` ({definitions}) CHARACTER SET utf8mb4")
                names = ", ".join(f"`{name}`" for name in columns)
                sql = f"INSERT INTO `{table}` ({names}) VALUES ({', '.join(['%s'] * len(columns))})"
                batch = []; total = 0
                for row in reader:
                    # Empty CSV cells represent missing values; all other bytes stay as text.
                    batch.append(tuple(row[name] if row[name] != "" else None for name in columns))
                    if len(batch) == 1000:
                        cursor.executemany(sql, batch); connection.commit()
                        total += len(batch); batch.clear()
                if batch:
                    cursor.executemany(sql, batch); connection.commit(); total += len(batch)
                cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
                assert cursor.fetchone()[0] == total
                audit["tables"][table] = {"rows": total, "columns": columns,
                    "url": SOURCES[table], "sha256": HASHES[table]}
                print(f"LOCAL MYSQL {table}: {total} rows, {len(columns)} columns", flush=True)
        target = PROJECT / "evidence/local_source.json"
        target.write_text(json.dumps(audit, indent=2) + "\n")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
