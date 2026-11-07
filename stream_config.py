"""Streaming connection settings come from the process environment only."""
import os
import re


def required(name):
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Set {name} before running the streaming pipeline")
    return value


def identifier(value):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError("Use a plain SQL identifier for the output schema and table")
    return value


def jdbc_options(prefix):
    host = required(prefix + "_HOST")
    port = os.getenv(prefix + "_PORT", "3306")
    schema = identifier(os.getenv(prefix + "_SCHEMA", "olympic_dataset"))
    tls = os.getenv(prefix + "_SSL_MODE", "VERIFY_IDENTITY")
    return {"url": f"jdbc:mysql://{host}:{port}/{schema}?connectionTimeZone=UTC&sslMode={tls}",
            "user": required(prefix + "_USER"), "password": required(prefix + "_PASSWORD"),
            "driver": "com.mysql.cj.jdbc.Driver"}


def kafka_options():
    return {"kafka.bootstrap.servers": os.getenv("FP_KAFKA_BROKER", "127.0.0.1:19095")}


def input_topic():
    return os.getenv("FP_INPUT_TOPIC", "athlete_event_results")


def output_topic():
    return os.getenv("FP_OUTPUT_TOPIC", "athlete_avg_stats_aurika_fp")


def output_table():
    return identifier(os.getenv("FP_OUTPUT_TABLE", "athlete_avg_stats_aurika_fp"))
