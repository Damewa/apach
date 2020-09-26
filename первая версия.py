from datetime import timedelta

import shutil
import os
import airflow
from zipfile import ZipFile
from shutil import copy
from shutil import move
from os import path
from shutil import make_archive
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}

src = 'E:/original/s.txt'
dst = 'E:/copy'

dag = DAG(
    'backup',
    default_args=default_args,
    description='backup',
    schedule_interval=timedelta(days=1),
)


def copy():
    dest = shutil.copy(src, dst)


def make_archive(source, destination):
    base = os.path.basename(destination)
    name = base.split('.')[0]
    format = base.split('.')[1]
    archive_from = os.path.dirname(source)
    archive_to = os.path.basename(source.strip(os.sep))
    shutil.make_archive(name, format, archive_from, archive_to)
    shutil.move('%s.%s' % (name, format), destination)
    make_archive(dst, dst)


run_this = PythonOperator(
    task_id='copy',
    python_callable=copy,
    dag=dag,
)

t2 = PythonOperator(
    task_id='make_arch',
    python_callable=make_archive,
    dag=dag,
)

run_this >> t2
