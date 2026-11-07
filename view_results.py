"""Read-only loopback viewer for actual MySQL SELECT and Kafka consumer output."""
import json
import os
from collections import Counter
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from common import PROJECT
from stream_config import output_topic, output_table
from verify_streaming import connection, topic_records


def mysql_text():
    table = output_table()
    client = connection("FP_OUTPUT_MYSQL")
    try:
        cursor = client.cursor(dictionary=True)
        summary_sql = ("SELECT batch_id, COUNT(*) AS groups_count, SUM(event_count) AS event_rows "
                       f"FROM `{table}` GROUP BY batch_id ORDER BY batch_id")
        cursor.execute(summary_sql)
        summaries = cursor.fetchall()
        query = ("SELECT sport, medal, sex, country_noc, avg_height, avg_weight, event_count, "
                 f"timestamp, batch_id FROM `{table}` "
                 f"WHERE batch_id = (SELECT MAX(batch_id) FROM `{table}`) "
                 "ORDER BY sport, medal, sex, country_noc LIMIT 2")
        cursor.execute(query)
        rows = cursor.fetchall()
    finally:
        client.close()
    lines = ["PART 1 | LIVE MYSQL RESULT", "Private local MySQL 127.0.0.1:13310 | schema aurika_fp",
             "Source: full official course CSV files imported into olympic_dataset",
             "Read time UTC: " + datetime.now(timezone.utc).isoformat(), "", summary_sql, ""]
    lines += [str(row) for row in summaries]
    lines += ["", query, ""]
    for number, row in enumerate(rows, 1):
        lines.append(f"--- Actual database row {number} ---")
        lines += [f"{name:14} | {value}" for name, value in row.items()]
        lines.append("")
    return "\n".join(lines)


def kafka_text():
    records = list(topic_records(output_topic()))
    batches = Counter(record.value["batch_id"] for record in records)
    latest = max(batches)
    sample = [record for record in records if record.value["batch_id"] == latest][:2]
    lines = ["PART 1 | LIVE KAFKA CONSUMER OUTPUT", "Broker: 127.0.0.1:19095",
             "Topic: " + output_topic(), "Read time UTC: " + datetime.now(timezone.utc).isoformat(),
             f"Actual stored messages: {len(records)} | groups per snapshot: {dict(sorted(batches.items()))}",
             "Two actual JSON records from the latest complete snapshot", ""]
    for record in sample:
        lines += [f"partition={record.partition} offset={record.offset}",
                  json.dumps(record.value, ensure_ascii=False, indent=2), ""]
    return "\n".join(lines)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in {"/mysql.txt", "/kafka.txt"}:
            self.send_error(404); return
        try:
            text = mysql_text() if self.path == "/mysql.txt" else kafka_text()
            name = "mysql-live.txt" if self.path == "/mysql.txt" else "kafka-live.txt"
            (PROJECT / "evidence" / name).write_text(text + "\n")
            data = text.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers(); self.wfile.write(data)
        except Exception:
            self.send_error(503, "The local result service is not available")
            raise


if __name__ == "__main__":
    print("Read-only actual results: http://127.0.0.1:18101/mysql.txt and /kafka.txt", flush=True)
    ThreadingHTTPServer(("127.0.0.1", int(os.getenv("FP_VIEW_PORT", "18101"))), Handler).serve_forever()
