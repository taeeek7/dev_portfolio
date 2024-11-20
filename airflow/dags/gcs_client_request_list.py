from airflow import DAG
from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from utils.slack import SlackUtils
from datetime import timedelta
import pendulum

# 실패 알림 메서드
def notify_failure(context) :
    SlackUtils.notify_failure(context)


# 데이터테이블 생성 정보
TARGET_DATASET = "cleanops" 
TABLE = "client_request_list"


with DAG(
    dag_id=f"gcs_{TABLE}", # dag_id - 보통 파일명과 동일하게
    schedule="1 11-21 * * *", # cron 스케줄
    start_date=pendulum.datetime(2024, 9, 20, 11, 1, tz="Asia/Seoul"), # 시작일자
    catchup=False, # 과거 데이터 소급적용
    tags=["gcs", "bigquery"], # 태그값
    default_args= {
        'on_failure_callback' : notify_failure,
        'retries': 2, #Task가 실패했을 때 재시도할 최대 횟수
        'retry_delay': timedelta(minutes=5), #재시도 간의 대기 시간을 지정
        'retry_exponential_backoff': False, #재시도 간격을 지수적으로 증가시킬지 여부를 결정
        'max_retry_delay': timedelta(minutes=30) #재시도 간격의 최대값을 지정합니다
    }
    
) as dag:
   
    # MySQL 데이터를 GCS로 내보내기
    export_mysql_to_gcs = MySQLToGCSOperator(
        task_id='export_mysql_to_gcs',
        mysql_conn_id= TARGET_DATASET,  # MySQL에 대한 Airflow Connection ID
        gcp_conn_id= 'bigquery-account', # GCS Airflow Connection ID
        sql= f'SELECT * FROM {TABLE} ;',  # MySQL에서 가져올 데이터 쿼리
        bucket='airflow-ops',  # 데이터를 저장할 GCS 버킷 이름
        filename=f'data/{TABLE}.json',  # GCS에 저장될 파일명 (JSON 형식)
        schema_filename=f'schema/schema_{TABLE}.json',  # (선택사항) 스키마 파일
        ensure_utc= True
    )

    # GCS 데이터를 BigQuery로 적재하기
    load_gcs_to_bigquery = GCSToBigQueryOperator(
        task_id='load_gcs_to_bigquery',
        gcp_conn_id= 'bigquery-account', # GCS Airflow Connection ID
        bucket='airflow-ops',  # GCS 버킷 이름
        source_objects=[f'data/{TABLE}.json'],  # GCS에 저장된 파일 이름
        destination_project_dataset_table=f'airflow-ops.{TARGET_DATASET}.{TABLE}',  # BigQuery 대상 테이블
        source_format='NEWLINE_DELIMITED_JSON',  # 파일 형식 (json)
        write_disposition='WRITE_TRUNCATE',  # 테이블에 데이터를 덮어쓰기
        create_disposition='CREATE_IF_NEEDED',
        schema_object=f'schema/schema_{TABLE}.json',  # GCS에 있는 스키마 파일 경로
    )

    # dag 작업 순서
    export_mysql_to_gcs >> load_gcs_to_bigquery
    

    

