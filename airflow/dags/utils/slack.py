from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dateutil.relativedelta import relativedelta
import os

class SlackUtils :
    # 생성자
    def __init__(self, *args) :
            self.slack_token = args[0]
            self.slack_channel = args[1]
    
    # 슬랙메시지발송 함수 
    def send_messages(self, text) : 
        ### 슬랙 API 및 메시지 발송 변수 설정 ###
        client = WebClient(token=self.slack_token)
        try : 
            response = client.chat_postMessage(channel= self.slack_channel, text= text)  
        except SlackApiError as e :
            assert e.response["error"]
        
    # slack Block-Kit 발송 함수
    def send_block_kit(self, text, blocks) : 
        ### 슬랙 API 및 메시지 발송 변수 설정 ###
        client = WebClient(token=self.slack_token)
        try : 
            response = client.chat_postMessage(channel= self.slack_channel, text= text, blocks= blocks)
        except SlackApiError as e :
            assert e.response["error"]
        
    # 스레드(reply) 발송 함수
    def send_threads(self, thread_ts, text) : 
        ### 슬랙 API 및 메시지 발송 변수 설정 ###
        client = WebClient(token=self.slack_token)
        try : 
            client.chat_postMessage(channel= self.slack_channel, thread_ts= thread_ts, text= text)
        except SlackApiError as e :
            assert e.response["error"]

    def success_msg(self, text) :
        client = WebClient(token=self.slack_token)
        client.chat_postMessage(channel= self.slack_channel, text= text)

    def fail_msg(self, text) :
        client = WebClient(token=self.slack_token)
        client.chat_postMessage(channel= self.slack_channel, text= text)
    
    def notify_failure(context):
        #환경변수 불러오기
        scheduler_token = os.getenv("AIRFLOW__SLACK__SCHEDULER")
        # 객체 생성
        slack_alert = SlackUtils(scheduler_token, "C07H689UA2K")

        # context에서 태스크 및 DAG 정보 가져오기
        task_instance = context.get('task_instance')
        task_id = task_instance.task_id
        dag_id = task_instance.dag_id
        execution_date = (context.get('execution_date') + relativedelta(hours=9)).strftime('%Y-%m-%d %H:%M:%S')
        log_url = task_instance.log_url
        domain_url = log_url.replace("http://localhost:8080", "airflow.11h.kr")

        # Slack 메시지 전송
        slack_alert.fail_msg(
            text= f"""
            💨 *Airflow Scheduler Alert*\n
            ● result: fail\n
            ● dag_id: {dag_id}\n
            ● task_id: {task_id}\n
            ● execution_date: {execution_date}\n
            ● log_url: {domain_url}
            """
        )