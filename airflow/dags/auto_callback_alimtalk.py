from __future__ import annotations
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from dateutil.relativedelta import relativedelta
from utils.slack import SlackUtils
from utils.sql import SqlUtils
from utils.alimtalk import AlimtalkUtils
from datetime import datetime
import pendulum
import os
import pandas as pd 

# 실패 알림
def notify_failure(context) :
    SlackUtils.notify_failure(context)

# 승인대기키퍼 추출
def wait_keeper_list() :
    data = SqlUtils.get_source_data(
        conn_id= "prod-keeper",
        sql= """
        select 
            b.name AS br_name,
            mk.member_keeper_id,
            mk.name AS kp_name,
            mk.phone,
            mk.state_code,
            REPLACE(REPLACE(b.kakao_link,'https://',''),'https://','') AS kakao_link,
            DATE_FORMAT(mk.insert_at, '%Y-%m-%d') AS insert_at
        from member_keeper as mk 
        LEFT JOIN branch b
            ON mk.cl_cd = b.cl_cd
            AND mk.branch_id = b.branch_id
        where 
            mk.LEVEL = 30
            and state_code = 'WAIT'
            and b.cl_cd not in ('Z0001')
            and b.branch_id IS NOT NULL
            and mk.insert_at between date_format(DATE_SUB(current_date(), interval 1 day), '%Y-%m-%d 00:00:00') and date_format(DATE_SUB(current_date(), interval 1 day), '%Y-%m-%d 23:59:59')
            and LEFT(b.kakao_link,1) = 'h'
        ORDER BY
            insert_at
        ;   
        """
    )
    return pd.DataFrame(data)


# 알림톡 전송 템플릿 
def callback_alimtalk_template(recipient_no, name, branch, ch_url) :
    ###비즈알림톡 API 변수 설정
    access_key = os.getenv("ALIMTALK_ACCESS_KEY")
    secret_key = os.getenv("ALIMTALK_SECRET_KEY")
    alimtalk_client = AlimtalkUtils(access_key, secret_key)

    ytb_url = "m.youtube.com/watch?v=MlMheHn0vJg"

    body = {
        "plusFriendId": "@열한시키퍼",
        "templateCode": "CallbackMessage",
        "messages": [
            {
                "to": f"{recipient_no}",
                "title": "안녕하세요 키퍼님",
                "content": f"안녕하세요 {name} 키퍼님, 열한시 클리닝 {branch}에 지원해 주셔서 감사합니다!\n\n업무 상담과 궁금하신 내용 문의는 [열한시클리닝_{branch}] 채널을 통해 진행됩니다.\n\n상담 시 성함과 연락처를 함께 남겨주시면 빠르고 정확한 답변이 가능하며, 이후 지점 담당자가 확인하여 답변드리도록 하겠습니다.\n\n답변을 기다리시는 동안 교육 영상 시청을 부탁드립니다.\n\n감사합니다.",
                "buttons": [
                    {
                        "type": "WL",
                        "name": "교육 영상 시청하기",
                        "linkMobile": f"https://{ytb_url}",
                        "linkPc": f"https://{ytb_url}"
                    },
                    {
                        "type": "WL",
                        "name": "지점 채널로 이동하기",
                        "linkMobile": f"https://{ch_url}",
                        "linkPc": f"https://{ch_url}"
                    },
                ],
                "useSmsFailover": False,
            }
        ],
    }

    response_text = alimtalk_client.send_alimtalk(body= body)

    return response_text



# 키퍼 알림톡 발송
def callback_alimtalk_handler() :
    send_df = wait_keeper_list()
    slack_token = os.getenv("AIRFLOW__SLACK__TOKEN")
    slack_channel = 'C05PKAP3PK6' 
    slack_client = SlackUtils(slack_token, slack_channel)

    ### 발송 변수 설정 ###
    now = (datetime.now() + relativedelta(hours=9)).strftime("%Y-%m-%d %H:%M:%S")
    success_cnt = 0 
    error_cnt = 0 
    error_code = []
    error_message = []
    error_keeper = []
    success_keeper = []

    if len(send_df) == 0 :
        print("발송건이 없습니다")
        return  
    else : 
        for i in range(0, len(send_df)) :
            branch= send_df.iloc[i,0]
            keeper= send_df.iloc[i,2]
            phone= send_df.iloc[i,3]
            kakao_link= send_df.iloc[i,5]
            
            
            response_text = callback_alimtalk_template(
                recipient_no= phone
                ,name= keeper
                ,branch= branch
                ,ch_url= kakao_link
            )

            response_code = response_text['statusCode']
            response_message = response_text['messages'][0]['requestStatusDesc']

            if response_code == '202' :
                #성공건수 count
                success_cnt = success_cnt + 1
                success_keeper.append(keeper)
            else :
                # 에러건수 count 
                error_cnt = error_cnt + 1
                error_message.append(response_message)
                error_code.append(response_code)
                error_keeper.append(keeper)


        slack_client.send_messages(
            text= f"💌 신규키퍼 알림톡 자동발송\n\n" 
                + f"   ● 실행일시 : {now}\n"
                + f"   ● 실행건수 : {len(send_df)} 건\n"
                + f"   ● 결과 : 성공 {success_cnt} 건  / 실패 {error_cnt} 건\n"
                + f"         ○ success_keeper : {success_keeper}\n"
                + f"         ○ error_code : {error_code}\n"
                + f"         ○ error_keeper : {error_keeper}"
        )
        
        return


with DAG(
    dag_id="auto_callback_alimtalk", # dag_id - 보통 파일명과 동일하게 
    schedule= "50 13 * * *", # cron 스케줄
    start_date=pendulum.datetime(2024, 10, 29, 13, 50, tz="Asia/Seoul"), # 시작일자
    catchup=False, # 과거 데이터 소급적용
    tags=["automation", "alimtalk", "callback"], # 태그값
    default_args= {
        'on_failure_callback' : notify_failure
    }
    
) as dag:
    # PythonOperator를 사용해 transfer_data 함수를 실행하는 작업 정의
    task_t1 = PythonOperator(
        task_id='task_t1',
        python_callable= callback_alimtalk_handler
    )

    task_t1
