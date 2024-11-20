from __future__ import annotations
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from utils.slack import SlackUtils 
from utils.sql import SqlUtils
import pendulum

# 실패 알림
def notify_failure(context) :
    SlackUtils.notify_failure(context)

# 데이터테이블 생성 정보
DATASET = "cleanops" 
TABLE = "keeper_state_log"
MART_DATASET = "mart"

def transfer_data() : 
    source_data = SqlUtils.get_source_data(
        conn_id= "prod-keeper",
        sql= """
        WITH temp_state_code AS ( 
        SELECT 
            DATE_FORMAT(CURRENT_DATE(), '%Y-%m-%d 00:00:00') AS date
            ,mk.member_keeper_id
            ,mk.state_code 
            ,CASE 
                WHEN mk.state_code = 'COMPLETE' THEN mk.grade_id	
                WHEN mk.state_code = 'STOP' THEN null
                WHEN mk.state_code = 'WAIT' THEN NULL
                WHEN mk.state_code = 'DELETE' THEN null
                END AS grade_id 
            ,mk.cl_cd
            ,mk.branch_id
        FROM member_keeper AS mk
        WHERE 
            mk.LEVEL = 30 
        ),
        temp_complete_cnt AS (
        SELECT 
            op.member_keeper_id  
            ,count(DISTINCT date_format(ko.end_at, '%Y-%m-%d')) AS work_cnt
        FROM order_party AS op 
        INNER JOIN keeper_order AS ko 
            ON op.keeper_order_id = ko.keeper_order_id 
        WHERE 
            ko.cancel_id IS NULL 
            AND ko.end_at IS NOT NULL 
        GROUP BY 1
        )
        SELECT 
            tcs.date
            ,tcs.state_code
            ,tcs.grade_id
            ,CASE 
                WHEN tcs.state_code = 'COMPLETE' AND tcc.work_cnt IS NULL THEN '0회'
                WHEN tcs.state_code = 'COMPLETE' AND tcc.work_cnt = 1 THEN '1회'
                WHEN tcs.state_code = 'COMPLETE' AND tcc.work_cnt = 2 THEN '2회'
                WHEN tcs.state_code = 'COMPLETE' AND tcc.work_cnt >= 3 THEN '3회이상'
                ELSE NULL 
                END AS work_cnt
            ,tcs.cl_cd
            ,tcs.branch_id
            ,count(tcs.member_keeper_id) AS keeper_cnt
        FROM temp_state_code AS tcs
        LEFT JOIN temp_complete_cnt AS tcc 
            ON tcs.member_keeper_id = tcc.member_keeper_id
        GROUP BY 1,2,3,4,5,6
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
            f"""INSERT INTO {TABLE} (date, state_code, grade_id, work_cnt, cl_cd, branch_id, keeper_cnt)
                VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                row
        )
    target_conn.commit()
    cursor.close()
    target_conn.close()

with DAG(
    dag_id=f"mart_{TABLE}", # dag_id - 보통 파일명과 동일하게 
    schedule="50 23 * * *", # cron 스케줄
    start_date=pendulum.datetime(2024, 9, 6, 23, 50, tz="Asia/Seoul"), # 시작일자
    catchup=False, # 과거 데이터 소급적용
    tags=["mart", "bigquery"], # 태그값
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
        destination_project_dataset_table=f'airflow-ops.{MART_DATASET}.{TABLE}',  # BigQuery 대상 테이블
        source_format='NEWLINE_DELIMITED_JSON',  # 파일 형식 (json)
        write_disposition='WRITE_TRUNCATE',  # 테이블에 데이터를 덮어쓰기
        create_disposition='CREATE_IF_NEEDED',
        schema_object=f'schema/schema_{TABLE}.json'  # GCS에 있는 스키마 파일 경로
    )
  
    transfer_t1 >> export_mysql_to_gcs >> load_gcs_to_bigquery
