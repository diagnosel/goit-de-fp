"""Check real MySQL and Kafka data against the complete original CSV files."""
import csv
import hashlib
import json
import math
import os
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
import mysql.connector
from kafka import KafkaConsumer, TopicPartition
from common import PROJECT, TABLES
from stream_config import input_topic, output_topic, kafka_options, output_table

KEYS = ("sport", "medal", "sex", "country_noc")


def connection(prefix):
    return mysql.connector.connect(host=os.environ[prefix + "_HOST"],
        port=int(os.environ[prefix + "_PORT"]), user=os.environ[prefix + "_USER"],
        password=os.environ[prefix + "_PASSWORD"], database=os.environ[prefix + "_SCHEMA"],
        time_zone="+00:00")


def fingerprint(row, columns):
    values = [row[name] for name in columns]
    return hashlib.sha256(json.dumps(values, ensure_ascii=False).encode()).digest()


def topic_records(topic):
    """Read a bounded snapshot. Never silently accept a consumer timeout as completion."""
    consumer = KafkaConsumer(bootstrap_servers=kafka_options()["kafka.bootstrap.servers"],
        group_id=None, enable_auto_commit=False, value_deserializer=lambda value: json.loads(value))
    try:
        partitions = sorted(consumer.partitions_for_topic(topic) or [])
        assert partitions == [0], "This local verification expects the configured single partition"
        targets = [TopicPartition(topic, number) for number in partitions]
        ends = consumer.end_offsets(targets)
        consumer.assign(targets)
        for target in targets:
            assert consumer.beginning_offsets([target])[target] == 0, "Source records have expired"
            consumer.seek(target, 0)
        deadline = time.monotonic() + 120
        while any(consumer.position(target) < ends[target] for target in targets):
            assert time.monotonic() < deadline, "Kafka snapshot was not read completely"
            for target, records in consumer.poll(timeout_ms=1000, max_records=3000).items():
                for record in records:
                    if record.offset < ends[target]:
                        yield record
    finally:
        consumer.close()


def snapshot_key(row):
    return (row["pipeline_run_id"], int(row["batch_id"])) + tuple(row[name] for name in KEYS)


def main():
    source = connection("FP_SOURCE_MYSQL")
    originals = {}; source_counts = {}; bio = {}; all_ids = set(); invalid = 0
    try:
        for table in TABLES:
            csv_dir = Path(os.getenv("FP_SOURCE_CSV_DIR", PROJECT / ".runtime/landing"))
            with (csv_dir / f"{table}.csv").open(encoding="utf-8-sig", newline="") as file:
                reader = csv.DictReader(file); columns = reader.fieldnames
                expected = Counter(); count = 0
                for raw in reader:
                    row = {name: value if value != "" else None for name, value in raw.items()}
                    expected[fingerprint(row, columns)] += 1; count += 1
                    if table == "athlete_bio":
                        assert row["athlete_id"] not in all_ids, "Duplicate dimension key"
                        all_ids.add(row["athlete_id"])
                        try:
                            height = Decimal(row["height"].strip()); weight = Decimal(row["weight"].strip())
                            if not height.is_finite() or not weight.is_finite():
                                raise ValueError("Nonfinite measurement")
                        except (AttributeError, InvalidOperation, ValueError):
                            invalid += 1; continue
                        bio[row["athlete_id"]] = (height, weight, row["sex"], row["country_noc"])
            cursor = source.cursor(dictionary=True)
            cursor.execute(f"SELECT * FROM `{table}`")
            actual = Counter(fingerprint(row, columns) for row in cursor)
            cursor.close()
            assert actual == expected, f"Local MySQL differs from official CSV: {table}"
            originals[table] = (columns, expected)
            source_counts[table] = count
    finally:
        source.close()

    output = connection("FP_OUTPUT_MYSQL")
    try:
        cursor = output.cursor(dictionary=True)
        cursor.execute(f"SELECT * FROM `{output_table()}`")
        mysql_rows = cursor.fetchall(); cursor.close()
    finally:
        output.close()
    kafka_rows = [record.value for record in topic_records(output_topic())]
    assert len(kafka_rows) == len(mysql_rows) > 0
    mysql = {snapshot_key(row): row for row in mysql_rows}
    kafka = {snapshot_key(row): row for row in kafka_rows}
    assert len(mysql) == len(mysql_rows), "Duplicate MySQL deliveries in this execution"
    assert len(kafka) == len(kafka_rows), "Duplicate Kafka deliveries in this execution"
    assert mysql.keys() == kafka.keys()
    for key, row in kafka.items():
        stored = mysql[key]
        for name in ("avg_height", "avg_weight"):
            assert math.isclose(row[name], stored[name], rel_tol=1e-12, abs_tol=1e-9)
        assert row["event_count"] == stored["event_count"]
        utc = datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00"))
        assert utc.replace(tzinfo=None) == stored["timestamp"], "Sink timestamps differ"

    checkpoint = Path(os.getenv("FP_CHECKPOINT", PROJECT / ".runtime/stream_checkpoint"))
    progress = json.loads((checkpoint / "progress.json").read_text())
    batch_ends = {}
    for item in progress:
        if item["numInputRows"]:
            offsets = item["sources"][0]["endOffset"]
            if isinstance(offsets, str):
                offsets = json.loads(offsets)
            batch_ends[offsets[input_topic()]["0"]] = item["batchId"]
    assert batch_ends, "No completed streaming batch progress"
    run_ids = {row["pipeline_run_id"] for row in kafka_rows}
    assert len(run_ids) == 1
    run_id = run_ids.pop()
    sums = defaultdict(lambda: [Decimal(0), Decimal(0), 0])
    seen = Counter(); columns, expected = originals["athlete_event_results"]
    unknown = invalid_events = total = 0; snapshots = []
    for record in topic_records(input_topic()):
        row = record.value
        assert set(row) == set(columns), "A JSON source field was lost"
        seen[fingerprint(row, columns)] += 1; total += 1
        athlete = row["athlete_id"]
        if athlete not in all_ids:
            unknown += 1
        elif athlete not in bio:
            invalid_events += 1
        else:
            height, weight, sex, country = bio[athlete]
            medal = row["medal"].strip(" ") if row["medal"] else None
            key = (row["sport"], medal or None, sex, country)
            sums[key][0] += height; sums[key][1] += weight; sums[key][2] += 1
        if record.offset + 1 in batch_ends:
            batch_id = batch_ends[record.offset + 1]
            snapshot = {key[2:]: value for key, value in kafka.items() if key[:2] == (run_id, batch_id)}
            assert snapshot.keys() == sums.keys()
            for key, (height, weight, count) in sums.items():
                actual = snapshot[key]
                assert actual["event_count"] == count
                assert math.isclose(actual["avg_height"], float(height / count), rel_tol=1e-12, abs_tol=1e-9), key
                assert math.isclose(actual["avg_weight"], float(weight / count), rel_tol=1e-12, abs_tol=1e-9), key
            snapshots.append({"batch_id": batch_id, "input_end_offset": record.offset + 1,
                              "groups_verified": len(snapshot)})
    assert seen == expected, "Input Kafka does not exactly match the official events CSV"
    assert total == max(batch_ends), "The stream did not consume every published event"
    assert len(snapshots) == len(batch_ends)
    joined = sum(value[2] for value in sums.values())
    assert unknown + invalid_events + joined == total
    sample = sorted(kafka_rows, key=lambda row: (row["batch_id"], str(tuple(row[k] for k in KEYS))))[-2:]
    audit = {"status": "PASS", "verified_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": "Private local MySQL imported from complete official course CSV files",
        "remote_course_mysql_used": False, "source_rows": source_counts,
        "source_fields_and_values_match_csv": True, "input_kafka_matches_all_source_rows": True,
        "valid_bio_rows": len(bio), "invalid_bio_rows": invalid,
        "input_events": total, "unknown_athlete_events": unknown,
        "events_with_invalid_bio": invalid_events, "joined_event_rows": joined,
        "final_groups": len(sums), "no_medal_groups": sum(key[1] is None for key in sums),
        "snapshots": snapshots, "kafka_history_rows": len(kafka_rows), "mysql_history_rows": len(mysql_rows),
        "all_snapshots_match_independent_decimal_means": True,
        "both_sinks_match_including_microsecond_timestamps": True, "sample": sample}
    (PROJECT / "evidence/streaming_verification.json").write_text(json.dumps(audit, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps(audit, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
