"""Create private accounts for a new project-only local MySQL server. Never reset existing settings."""
from pathlib import Path
import os,secrets,shlex
import mysql.connector
root=Path(__file__).resolve().parent
if (root/'.runtime/local.env.sh').exists():
 raise ValueError('Private settings already exist. Use a new project runtime.')
root_password=secrets.token_hex(24);source_password=secrets.token_hex(24);output_password=secrets.token_hex(24)
connection=mysql.connector.connect(host='127.0.0.1',port=13310,user='root',password='')
cursor=connection.cursor()
cursor.execute('SELECT @@datadir')
if Path(cursor.fetchone()[0]).resolve() != (root/'.runtime/mysql-data-local').resolve():
 raise ValueError('This helper is only for the new project MySQL datadir')
cursor.execute("SHOW DATABASES LIKE 'olympic_dataset'")
if cursor.fetchone():
 raise ValueError('Source schema already exists. Do not replace an existing database.')
cursor.execute('CREATE DATABASE olympic_dataset CHARACTER SET utf8mb4')
cursor.execute('CREATE DATABASE aurika_fp CHARACTER SET utf8mb4')
for name,password,grants,schema in [('fp_source',source_password,'SELECT','olympic_dataset'),('fp_output',output_password,'SELECT, INSERT, CREATE','aurika_fp')]:
 cursor.execute(f"CREATE USER '{name}'@'localhost' IDENTIFIED BY %s",(password,))
 cursor.execute(f"GRANT {grants} ON {schema}.* TO '{name}'@'localhost'")
cursor.execute('''CREATE TABLE aurika_fp.athlete_avg_stats_aurika_fp (
 sport VARCHAR(255), medal VARCHAR(32), sex VARCHAR(32), country_noc VARCHAR(32),
 avg_height DOUBLE, avg_weight DOUBLE, event_count BIGINT,
 timestamp TIMESTAMP(6) NULL, pipeline_run_id VARCHAR(64), batch_id BIGINT
) CHARACTER SET utf8mb4''')
cursor.execute("ALTER USER 'root'@'localhost' IDENTIFIED BY %s",(root_password,))
connection.close()
values={'FP_SOURCE_MYSQL_HOST':'127.0.0.1','FP_SOURCE_MYSQL_PORT':'13310','FP_SOURCE_MYSQL_SCHEMA':'olympic_dataset','FP_SOURCE_MYSQL_USER':'fp_source','FP_SOURCE_MYSQL_PASSWORD':source_password,'FP_SOURCE_MYSQL_SSL_MODE':'REQUIRED','FP_OUTPUT_MYSQL_HOST':'127.0.0.1','FP_OUTPUT_MYSQL_PORT':'13310','FP_OUTPUT_MYSQL_SCHEMA':'aurika_fp','FP_OUTPUT_MYSQL_USER':'fp_output','FP_OUTPUT_MYSQL_PASSWORD':output_password,'FP_OUTPUT_MYSQL_SSL_MODE':'REQUIRED','FP_IMPORT_MYSQL_USER':'root','FP_IMPORT_MYSQL_PASSWORD':root_password,'FP_KAFKA_BROKER':'127.0.0.1:19095','FP_INPUT_TOPIC':'athlete_event_results','FP_OUTPUT_TOPIC':'athlete_avg_stats_aurika_fp','JAVA_HOME':'/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home','SPARK_LOCAL_IP':'127.0.0.1','PYSPARK_PYTHON':__import__('sys').executable,'PYTHONUNBUFFERED':'1'}
p=root/'.runtime/local.env.sh'
p.write_text(''.join(f'export {k}={shlex.quote(v)}\n' for k,v in values.items()));p.chmod(0o600)
print('Private local users and output table prepared. Credentials saved in runtime only.')
