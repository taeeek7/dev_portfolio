from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.slack import SlackUtils
from utils.sql import SqlUtils
from utils.aws import AwsUtils
from protocolbufs.generated import slack_pb2
from google.protobuf.json_format import MessageToDict
import pendulum
import base64
import os 

# 실패 알림
def notify_failure(context) :
    SlackUtils.notify_failure(context)

# SQS 메시지 수신 함수
def process_sqs_handler() :
    aws_client = AwsUtils('aws_conn_profile', 'ap-northeast-2')
    sqs_url = f"{os.getenv("AIRFLOW__SQS__URL")}/GradeUp-SQS"
    message = aws_client.receive_sqs_message(sqs_url= sqs_url)

    body = message['Body']
    # decode base64
    decoding = base64.b64decode(body)
    
    # 메시지 처리 (예: protobuf data Deserialize)
    grade_up_event = slack_pb2.GradeUpEvent()
    grade_up_event.ParseFromString(decoding)

    # protobuf 메시지를 JSON 형태로 변환
    event_data = MessageToDict(grade_up_event)

    slack_block_id = event_data['blockId']
    action_time = event_data['actionTime']
    text_value = event_data['textValue'].replace("+", " ")
    member_keeper_id = event_data['memberKeeperId']
    select_type = event_data['selectType']
    username = event_data['username']

    SqlUtils.crud_data(
        conn_id= 'cleanops',
        sql= f"""
        REPLACE INTO slack_actions_log (slack_block_id,action_time,text_value,member_keeper_id,select_type,username)
        VALUES ("{slack_block_id}","{action_time}","{text_value}",{member_keeper_id},"{select_type}","{username}")
        """
    )
    return event_data

    
with DAG(
    dag_id="sqs_trigger_grade_up", # dag_id - 보통 파일명과 동일하게 
    schedule= None, # cron 스케줄
    start_date=pendulum.datetime(2024, 8, 27, tz="Asia/Seoul"), # 시작일자
    catchup=False, # 과거 데이터 소급적용
    tags=["sqs", "grade_up"], # 태그값
    default_args= {
        'on_failure_callback' : notify_failure
    }
) as dag :
    process_message = PythonOperator(
        task_id='process_message',
        python_callable=process_sqs_handler,
        provide_context=True,
    )

    process_message