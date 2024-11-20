from __future__ import annotations
from airflow.models.dag import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from dateutil.relativedelta import relativedelta
from utils.slack import SlackUtils 
import pendulum
import os 

def notify_failure(context):
    SlackUtils.notify_failure(context)

# Python 함수로 데이터 추출 및 삽입 작업 정의
def transfer_data():
    # 1. 소스 DB에서 데이터 추출
    source_hook = MySqlHook(mysql_conn_id='prod-keeper')
    source_data = source_hook.get_records(
		sql="""
		SELECT  *
        FROM keeper.room r 
        WHERE (
        DATE_FORMAT(insert_at, '%Y-%m-%d') = DATE_FORMAT(DATE_SUB(CURRENT_DATE(), INTERVAL 1 day), '%Y-%m-%d')
        OR DATE_FORMAT(update_at, '%Y-%m-%d') = DATE_FORMAT(DATE_SUB(CURRENT_DATE(), INTERVAL 1 day), '%Y-%m-%d')
        OR (status in ('work', 'active') and is_delete = 0 )
        )
        ;
		"""
	)
    
    # 2. 타겟 DB로 데이터 삽입
    target_hook = MySqlHook(mysql_conn_id='cleanops')
    target_conn = target_hook.get_conn()
    cursor = target_conn.cursor()

    # 데이터를 target_table에 삽입
    for row in source_data:
        cursor.execute(
            """REPLACE INTO room 
			    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            row
        )

    target_conn.commit()
    cursor.close()
    target_conn.close()

# 데이터테이블 생성 정보
DATASET = "cleanops" 
TABLE = "room"

with DAG(
    dag_id=f"keeper_{TABLE}_sync", # dag_id - 보통 파일명과 동일하게 
    schedule="5 4 * * *", # cron 스케줄
    start_date=pendulum.datetime(2024, 8, 12, tz="Asia/Seoul"), # 시작일자
    catchup=False, # 과거 데이터 소급적용
    tags=["keeper", "sync"], # 태그값
    default_args= {
        'on_failure_callback' : notify_failure
    }
    
) as dag:
    # PythonOperator를 사용해 transfer_data 함수를 실행하는 작업 정의
    transfer_t1 = PythonOperator(
        task_id='transfer_t1',
        python_callable=transfer_data,
    )

    # MySQL 데이터를 GCS로 내보내기
    export_mysql_to_gcs = MySQLToGCSOperator(
        task_id='export_mysql_to_gcs',
        mysql_conn_id='cleanops',  # MySQL에 대한 Airflow Connection ID
        gcp_conn_id= 'bigquery-account', # GCS Airflow Connection ID
        sql= f'SELECT * FROM {TABLE} ;',  # MySQL에서 가져올 데이터 쿼리
        bucket='airflow-ops',  # 데이터를 저장할 GCS 버킷 이름
        filename=f'data/{TABLE}.json',  # GCS에 저장될 파일명 (JSON 형식)
        schema_filename=f'schema/schema_{TABLE}.json',  # (선택사항) 스키마 파일
    )

    # GCS 데이터를 BigQuery로 적재하기
    load_gcs_to_bigquery = GCSToBigQueryOperator(
        task_id='load_gcs_to_bigquery',
        gcp_conn_id= 'bigquery-account', # GCS Airflow Connection ID
        bucket='airflow-ops',  # GCS 버킷 이름
        source_objects=[f'data/{TABLE}.json'],  # GCS에 저장된 파일 이름
        destination_project_dataset_table=f'airflow-ops.{DATASET}.{TABLE}',  # BigQuery 대상 테이블
        source_format='NEWLINE_DELIMITED_JSON',  # 파일 형식 (json)
        write_disposition='WRITE_TRUNCATE',  # 테이블에 데이터를 덮어쓰기
        create_disposition='CREATE_IF_NEEDED',
        schema_object=f'schema/schema_{TABLE}.json'  # GCS에 있는 스키마 파일 경로
    )

    transfer_t1 >> export_mysql_to_gcs >> load_gcs_to_bigquery
