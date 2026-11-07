"""Part 2 stage 4: three sequential Airflow tasks run the actual Spark scripts."""
import os
import shlex
from pathlib import Path
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = Path(__file__).resolve().parent
PYTHON = os.getenv("FP_SPARK_PYTHON", str(PROJECT / ".venv" / "bin" / "python"))

with DAG("aurika_fp_batch_lake", start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
         schedule=None, catchup=False, max_active_runs=1,
         default_args={"retries": 0},
         params={"lake_root": os.getenv("FP_LAKE_ROOT", str(PROJECT / "lake"))},
         tags=["data-engineering", "final-project"]) as dag:
    tasks = [BashOperator(task_id=name,
                          bash_command=f"{shlex.quote(PYTHON)} {shlex.quote(str(PROJECT / (name + '.py')))}",
                          cwd=str(PROJECT), append_env=True,
                          env={"FP_LAKE_ROOT": "{{ dag_run.conf.get('lake_root', params.lake_root) }}"})
             for name in ["landing_to_bronze", "bronze_to_silver", "silver_to_gold"]]
    tasks[0] >> tasks[1] >> tasks[2]
