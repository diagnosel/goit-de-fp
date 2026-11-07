# Final project on athlete streaming and a batch data lake

Author: Григорова Ауріка Вадимівна.

Both parts run locally on the full official course CSV files. **The streaming source is a private local MySQL, not the remote course MySQL.** Both original tables, with all their columns, were imported into `olympic_dataset`. Spark then read those MySQL tables through JDBC. Kafka and the output MySQL are also local. This source choice is stated in the screenshot report.

## Results

The final streaming run processed 316,834 events in two microbatches. Each batch wrote a complete snapshot of cumulative means to Kafka and MySQL. Batch 0 has 6,589 groups; batch 1 has 10,905 groups. Each sink has 17,494 history rows. The latest snapshot represents 226,887 matched event rows.

Three full Airflow scheduler runs completed the batch pipeline. The latest run is `fp_final_03`. Every run has three successful tasks. Independent CSV and Decimal calculations check every persisted Gold group. A separate verifier checks all original CSV values in MySQL and Kafka, both streaming snapshots, counts, means and microsecond timestamps.

| Check | Streaming | Batch |
| --- | ---: | ---: |
| Original athlete rows | 155861 | 155861 |
| Original event rows | 316834 | 316834 |
| Event rows after Silver deduplication | Not applied | 315626 |
| Valid athlete rows | 104153 | 104153 |
| Athlete rows excluded for invalid height or weight | 51708 | 51708 |
| Joined event rows used for means | 226887 | 226872 |
| Final groups | 10905 | 10905 |

Streaming preserves the original event rows. Batch removes 1,208 duplicate full rows after text cleaning. Only 15 of those duplicates have valid measurements and enter the join. Therefore the two pipelines can have different means, even though they have the same number of final groups. They must not be forced to match.

The final output topic is `athlete_avg_stats_aurika_fp_final`. The final table is `aurika_fp.athlete_avg_stats_final`. The first attempt is preserved separately and is not used as the final result. See `results/` for the actual checks.

## Data rules

- Official sources: [athlete_bio.csv](https://ftp.goit.study/neoversity/athlete_bio.csv) and [athlete_event_results.csv](https://ftp.goit.study/neoversity/athlete_event_results.csv).
- All source fields are imported as text. Empty CSV cells become SQL NULL. Every other source value is preserved. The importer checks the official file hashes and refuses to replace existing source tables.
- Height and weight lose edge whitespace, including nonbreaking spaces, before `try_cast(... as double)`. Missing, nonnumeric, NaN and infinite values are excluded. No extra age or positive-value rule is added.
- Silver trims and collapses whitespace in every text column. Unicode letters and punctuation stay unchanged. It removes duplicate full rows, not repeated athlete IDs.
- The athlete dimension must have unique `athlete_id` values. An inner join keeps events that have a bio with valid height and weight. The verifiers report missing keys and events linked to invalid measurements.
- Grouping uses `sport`, `medal`, `sex` and **athlete_bio.country_noc**. The event delegation country is a separate source field. The assignment does not choose between them, so this choice is explicit.
- NULL and empty medal values form the no-medal group. These rows are not removed.
- Means are weighted by event-result rows, not distinct athletes. Height is in centimetres and weight in kilograms. Double precision is used without early rounding. Decimal is used in the independent check.
- `timestamp` is the UTC calculation time. `event_count`, `pipeline_run_id` and `batch_id` are extra control fields. The output MySQL table uses `TIMESTAMP(6)` to preserve the same precision as JSON.

Source SHA-256 values:

```text
athlete_bio.csv
63781c43de4b7376f690b96e86063854034b2e4477a7e16e617a71c26623d356
athlete_event_results.csv
4a76c66fc6f69a2f69b4a953298d475fd61fb46a40ac5995ea738bc94695905b
```

## Environment

Tested on macOS ARM64 with Python 3.12.4, Java 17.0.17, PySpark 4.0.1, Kafka 4.0.2, MySQL 8.4.11 and Airflow 2.11.2. MySQL Connector Python is 9.6.0. Spark uses Kafka connector `4.0.1` for Scala `2.13` and MySQL JDBC driver `8.4.0`. Airflow has a separate Python environment with SQLite and SequentialExecutor. This is a local teaching setup.

From this project folder:

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
export FP_SPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_PYTHON="$FP_SPARK_PYTHON"
export JAVA_HOME=/path/to/java17
export SPARK_LOCAL_IP=127.0.0.1
export PYTHONUNBUFFERED=1
mkdir -p .runtime
```

Replace `JAVA_HOME` with your JDK 17 folder. The checked Mac used `/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home`.

## Part 2 run the batch lake

1. `landing_to_bronze.py` downloads unchanged CSV files and writes both tables to `bronze/{table}` as Parquet.
2. `bronze_to_silver.py` cleans all text columns, removes full duplicates and writes `silver/{table}`.
3. `silver_to_gold.py` validates measurements, joins on athlete_id, calculates means and writes `gold/avg_stats` with a UTC timestamp.
4. `project_solution.py` runs those scripts in order through three Airflow BashOperator tasks.

Create a separate Airflow environment:

```bash
python3.12 -m venv .venv-airflow
.venv-airflow/bin/python -m pip install -r requirements-airflow.txt \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.11.2/constraints-3.12.txt
.venv-airflow/bin/python -m pip check
export PATH="$PWD/.venv-airflow/bin:$PATH"
export AIRFLOW_HOME="$PWD/.runtime/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
export FP_LAKE_ROOT="$PWD/lake"
airflow db migrate
airflow scheduler
```

In a second terminal, export the same settings, then run:

```bash
airflow dags unpause aurika_fp_batch_lake
airflow dags trigger aurika_fp_batch_lake --run-id my_first_run
airflow webserver --hostname 127.0.0.1 --port 18100
```

Configure your local UI account before using the webserver. The evidence uses a new loopback-only UI with read-only Viewer access. Every Spark task prints `df.show()`, plus a vertical display so all columns fit in a screenshot. Actual task logs provide the time column.

Use a new empty lake folder for every full run. The DAG accepts `lake_root` in the run config:

```json
{"lake_root":"/absolute/path/to/goit-de-fp/lake_repeat_02"}
```

Existing Parquet output folders are not overwritten. Reusing nonempty output folders fails. Original downloaded CSV files can be reused unchanged. For a direct Spark run, execute the three Python files in the same order with the Spark environment.

## Part 1 prepare the local services

Use a new private MySQL server on `127.0.0.1:13310` and a new Kafka broker on `127.0.0.1:19095`, with controller port `19096`. Do not reuse another project's database or broker. No system service or launchd job is needed.

<details>
<summary>Portable MySQL commands used on macOS ARM64</summary>

Download the [official MySQL 8.4.11 archive](https://cdn.mysql.com/archives/mysql-8.4/mysql-8.4.11-macos15-arm64.tar.gz) and its [publisher MD5](https://cdn.mysql.com/archives/mysql-8.4/mysql-8.4.11-macos15-arm64.tar.gz.md5). Check the hash before extraction. MD5 here checks download integrity; it is not a security guarantee on its own. The archive is fetched over HTTPS from the vendor.

```bash
mkdir -p .runtime/services
curl -fL https://cdn.mysql.com/archives/mysql-8.4/mysql-8.4.11-macos15-arm64.tar.gz -o .runtime/mysql.tar.gz
curl -fL https://cdn.mysql.com/archives/mysql-8.4/mysql-8.4.11-macos15-arm64.tar.gz.md5 -o .runtime/mysql.md5
md5 -q .runtime/mysql.tar.gz
cat .runtime/mysql.md5
# Continue only if the hashes match.
tar -xzf .runtime/mysql.tar.gz -C .runtime/services
export MYSQL_HOME="$PWD/.runtime/services/mysql-8.4.11-macos15-arm64"
mkdir -m 700 .runtime/mysql-data-local
"$MYSQL_HOME/bin/mysqld" --no-defaults --initialize-insecure \
  --basedir="$MYSQL_HOME" --datadir="$PWD/.runtime/mysql-data-local"
"$MYSQL_HOME/bin/mysqld" --no-defaults --basedir="$MYSQL_HOME" \
  --datadir="$PWD/.runtime/mysql-data-local" --bind-address=127.0.0.1 \
  --port=13310 --mysqlx=OFF --socket=/private/tmp/aurika-fp-mysql.sock \
  --default-time-zone=+00:00
```

The initialization starts with a temporary empty root password. In another terminal, run the setup immediately. It checks the exact project datadir, creates private users and replaces the empty root password with a random password. It refuses to reset an existing setup. Source access is SELECT only for the Spark reader. Output access is limited to the result schema.

```bash
.venv/bin/python setup_local_mysql.py
source .runtime/local.env.sh
export FP_SOURCE_CSV_DIR="$FP_LAKE_ROOT/landing"
.venv/bin/python import_local_source.py
```

Keep `.runtime/local.env.sh` private. It has mode 600 and is excluded from Git and the final archive. The setup script does not copy real passwords into code. For another OS, prepare an equivalent private MySQL 8 server and set your own connection variables from `.env.example`. The small Mac bootstrap helper is specific to the documented project datadir.
</details>

<details>
<summary>Local Kafka setup</summary>

Download [Kafka 4.0.2](https://archive.apache.org/dist/kafka/4.0.2/kafka_2.13-4.0.2.tgz) and its [SHA-512 file](https://archive.apache.org/dist/kafka/4.0.2/kafka_2.13-4.0.2.tgz.sha512). Check the hash and extract to `.runtime/services`. Set `KAFKA_HOME` to that extracted folder. Put the following in `.runtime/kafka.properties`, replacing the log directory with your absolute project path:

```properties
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:19096
listeners=PLAINTEXT://127.0.0.1:19095,CONTROLLER://127.0.0.1:19096
advertised.listeners=PLAINTEXT://127.0.0.1:19095
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs=/absolute/path/to/goit-de-fp/.runtime/kafka-data
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=false
```

```bash
"$KAFKA_HOME/bin/kafka-storage.sh" random-uuid > .runtime/kafka-cluster-id
"$KAFKA_HOME/bin/kafka-storage.sh" format \
  -t "$(cat .runtime/kafka-cluster-id)" -c .runtime/kafka.properties
export KAFKA_HEAP_OPTS='-Xms256M -Xmx512M'
"$KAFKA_HOME/bin/kafka-server-start.sh" .runtime/kafka.properties
```

Format only a new empty project store. Kafka PLAINTEXT is limited to loopback for this local exercise. Production access would need separate security settings.
</details>

## Part 1 run Spark streaming

With the local services running, export your private settings in each terminal. The scripts read process environment variables; they do not load `.env` automatically. The default JDBC mode for a remote connection is VERIFY_IDENTITY. The local test uses TLS with REQUIRED on its own server.

```bash
source .runtime/local.env.sh
export FP_SOURCE_CSV_DIR="$FP_LAKE_ROOT/landing"
.venv/bin/python create_stream_topics.py
export FP_PACKAGES=org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,com.mysql:mysql-connector-j:8.4.0
.venv/bin/spark-submit --packages "$FP_PACKAGES" --conf "spark.jars.ivy=$PWD/.runtime/ivy" mysql_to_kafka.py
.venv/bin/spark-submit --packages "$FP_PACKAGES" --conf "spark.jars.ivy=$PWD/.runtime/ivy" streaming_pipeline.py --available-now
.venv/bin/python verify_streaming.py
```

The numbered code comments identify all six assignment stages. `--available-now` handles the available Kafka records, then ends. Without it, the query keeps running. The default limit is 160,000 offsets per trigger, giving two microbatches for this source. `foreachBatch` writes the same cached snapshot to both sinks. For a new custom table, create the columns shown in `setup_local_mysql.py`, including `TIMESTAMP(6)`. Otherwise JDBC can create a timestamp column with lower precision.

The final checked run used a fresh output topic, table and checkpoint after fixing progress JSON serialization. These settings were `FP_OUTPUT_TOPIC=athlete_avg_stats_aurika_fp_final`, `FP_OUTPUT_TABLE=athlete_avg_stats_final`, and `FP_CHECKPOINT` ending in `.runtime/stream_checkpoint_final`.

Do not publish the input CSV again into an existing topic for a clean replay. A new experiment needs new topics, output table and checkpoint. To resume, keep the same checkpoint and do not republish old source events. The completed resume check sent no new events and added no output rows. This does not prove crash recovery or exactly-once delivery.

**Delivery is at least once.** Kafka and MySQL do not share a transaction. A failure between writes can leave one sink ahead of the other. A retry can add duplicates. Run and batch IDs identify snapshots but do not remove duplicates. Complete mode stores full cumulative snapshots; use the largest batch_id within a run for the latest values. The query has no time window or state retention policy for an unlimited production stream.

## Checks and evidence

```bash
.venv/bin/python tests/test_cleaning.py
.venv/bin/python verify_batch.py
.venv/bin/python verify_streaming.py
.venv/bin/python consume_stream_results.py
```

Run `verify_batch.py` with `FP_LAKE_ROOT` pointing to a completed lake. Run `verify_streaming.py` with the same source CSV directory, checkpoint and connection settings used by the stream. It checks each actual Kafka snapshot against a separate Python/Decimal calculation. Input message fingerprints must match every CSV row, including duplicates. Local MySQL source fields must match those same CSV values. Neither check uses Spark groupBy as its oracle.

`view_results.py` serves live SELECT and Kafka reads at `http://127.0.0.1:18101/mysql.txt` and `/kafka.txt`. Set `FP_VIEW_PORT` to change the port. This is a read-only plain-text viewer, not the native MySQL UI. The streaming screenshots were captured from that viewer on port 18102. The batch screenshots come from the actual Airflow UI.

| File | Contents |
| --- | --- |
| `screenshots/part1_01_mysql.jpg` | Live database SELECT and two final rows |
| `screenshots/part1_02_kafka.jpg` | Actual Kafka JSON records with offsets |
| `screenshots/part2_01_bronze.jpg` | Airflow task log with df.show and time |
| `screenshots/part2_02_silver.jpg` | Airflow task log with df.show and time |
| `screenshots/part2_03_gold.jpg` | Airflow task log with saved Gold rows and time |
| `screenshots/part2_04_dag.jpg` | Completed Airflow DAG graph |
| `results/` | Selected machine-readable checks and source hashes |
| `ФП_Григорова_Ауріка_Вадимівна.docx` | Combined report with all six screenshots |

Three Spark tests cover text and Unicode, full duplicates, missing and invalid numbers, no-medal groups, country choice, unmatched IDs and raw numeric nonbreaking spaces. A real worked example is the USA women's 3x3 basketball Gold group: the valid rows are 196 cm / 105 kg and 183 cm / 76 kg. The result is 189.5 cm, 90.5 kg and event_count 2. Two other source participants lack measurements and are excluded.

The final ZIP includes source code, requirements, README, results, all six screenshots and the combined Word document. Runtime binaries, database files, environments, private settings and large logs are excluded.

## Issues resolved during the run

- MySQL initialization inside the restricted process sandbox failed before creating InnoDB files. A new datadir was initialized outside that sandbox. The incomplete datadir was preserved and is not used.
- JDBC without TLS rejected public-key retrieval. The local connection was changed to TLS REQUIRED. Remote connections retain VERIFY_IDENTITY by default.
- Raw numeric values can contain nonbreaking spaces at their edges. Edge whitespace is removed before numeric parsing. A focused Spark test confirms this.
- Spark 4 progress dictionaries contain UUID objects and JSON strings for offsets. Progress saving converts UUID to text, and the verifier parses string offsets. A fresh stream used new output targets after the first attempt.
- The initial Airflow scheduler needed its environment bin folder in PATH. All completed scheduler runs use that corrected environment.

## References

- [Spark Structured Streaming](https://spark.apache.org/docs/4.0.1/streaming/apis-on-dataframes-and-datasets.html)
- [Spark JDBC](https://spark.apache.org/docs/4.0.1/sql-data-sources-jdbc.html)
- [Kafka quick start](https://kafka.apache.org/40/getting-started/quickstart/)
- [MySQL data directory setup](https://dev.mysql.com/doc/refman/8.4/en/data-directory-initialization.html)
- [Airflow DAG runs](https://airflow.apache.org/docs/apache-airflow/2.11.0/core-concepts/dag-run.html)
- [Reference for the local source approach](https://github.com/slipkoliudmyla/goit-de-fp). This project uses its own code, runs and evidence.

Beginner explanation in Ukrainian with the actual screenshots and code: [Notion guide](https://app.notion.com/p/411a96dc80e58113a783d3d0dcdd50cf).
