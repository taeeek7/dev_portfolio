from __future__ import annotations
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils.slack import SlackUtils
from utils.gsheet import GsheetUtils
from utils.keeper_api import KeeperApiUtils
from dateutil.relativedelta import relativedelta
import pendulum
import os

# Google Sheets 스프레드시트 ID 및 시트 이름 설정
SHEET_KEY = '/opt/airflow/services/keeper-data-4c16ed1166b5.json'
SHEET_ID = '1koZ1wJV63Si1H_hlHqvQY9Uweh9OYLJ3LIKY1PiGBgA'
FORMAT_SHEET_NAME = '긴급티켓발행양식'
UPLOAD_SHEET_NAME = 'python_upload'
SLACK_CHANNEL = 'C077ASTJ596'

# 실패 알림
def notify_failure(context) :
    SlackUtils.notify_failure(context)

# 티켓 리스트 구글 시트에서 읽고 API 요청
def insert_ticket_handler() :
    # 환경변수
    slack_token = os.getenv("AIRFLOW__SLACK__TOKEN")
    keeper_url = os.getenv("PROD_KEEPER_API") ### PROD로 변경필요!!!!!
    response_date = (datetime.now() + relativedelta(hours=9)).strftime("%Y-%m-%d %H:%M:%S")
    keeper_client = KeeperApiUtils(keeper_url)
    slack_client = SlackUtils(slack_token, SLACK_CHANNEL)

    # 양식 시트 읽기
    branch = GsheetUtils.read_cell_point(
        sheet_key= SHEET_KEY,
        sheet_id= SHEET_ID,
        sheet_name= FORMAT_SHEET_NAME,
        cell= 'B1'
    )

    issue_status = GsheetUtils.read_cell_point(
        sheet_key= SHEET_KEY,
        sheet_id= SHEET_ID,
        sheet_name= FORMAT_SHEET_NAME,
        cell= 'B4'
    )

    # 업로드 시트
    upload_df = GsheetUtils.read_gsheet(
        sheet_key= SHEET_KEY,
        sheet_id= SHEET_ID,
        sheet_name= UPLOAD_SHEET_NAME
    )
    
    # API 호출 결과값 설정 변수
    success_cnt = 0
    error_cnt =  0
    assign_cnt = 0
    complete_y = 0 
    error_roomNo = []
    error_data = []
    complete_room = []

    if issue_status == '발행대기' : 
        # 티켓생성 반복문
        for i in range(0, len(upload_df)) : 
            
            # 사용할 변수 값을 설정
            clCd = upload_df.loc[i,0]
            branchId = upload_df.loc[i,1]
            roomId = upload_df.loc[i,2]
            ticketCode = upload_df.loc[i,3]
            emergencyCode = upload_df.loc[i,4]
            emergencyComment = upload_df.loc[i,5]
            searchDate = upload_df.loc[i,6]
            roomNo = upload_df.loc[i,7]
            memberKeeperId = upload_df.loc[i,8]
            complete_yn = upload_df.loc[i,9]

            # POST 요청 보내기
            if complete_yn == '0' : 
                result = keeper_client.post_insert_ticket(
                    endPoint= "v1/ticket/setEmergencyTicket"
                    ,clCd= clCd
                    ,branchId= branchId
                    ,roomId= roomId
                    ,ticketCode= ticketCode
                    ,emergencyCode= emergencyCode
                    ,emergencyComment= emergencyComment
                    ,searchDate= searchDate
                    ,memberKeeperId= memberKeeperId
                )

                # API 에러 여부 확인 (단건 조회)
                if result['error'] == None :
                    success_cnt = success_cnt + 1
                    if memberKeeperId == '' :
                        assign_cnt = assign_cnt + 0 
                    else : 
                        assign_cnt = assign_cnt + 1 
                else : 
                    error_roomNo.append(roomNo)
                    error_data.append(result['error']['detail'])
                    error_cnt = error_cnt + 1
            else : 
                complete_y = complete_y + 1
                complete_room.append(roomNo)
        
        # API 호출 결과 슬랙메시지 발송
        slack_client.send_messages(
            text=  f"🔖 *{branch} 긴급티켓 자동발행*\n\n" 
                    + f"   ● 실행일시 : {response_date}\n"
                    + f"   ● 실행건수 : {len(upload_df)} 건\n"
                    + f"   ● 결과 : 발행완료 총 {success_cnt} 건 (배정 {assign_cnt} 건)  //  발행실패 {error_cnt} 건  //  수행이력 존재 {complete_y} 건\n"
                    + f"         ○ error_roomNo : {error_roomNo}\n"
                    + f"         ○ error_data : {error_data}\n"
                    + f"         ○ complete_yes_roomNo : {complete_room}"
        )

        # 구글 시트 업데이트
        if error_cnt >= 1 : 
            GsheetUtils.update_cell_point(sheet_key= SHEET_KEY, sheet_id= SHEET_ID, sheet_name= FORMAT_SHEET_NAME, cell= 'B4', update_value= "발행중지")
            print(f"발행건에 오류가 있습니다.({error_cnt})")
            return
        
        else : 
            GsheetUtils.update_cell_point(sheet_key= SHEET_KEY, sheet_id= SHEET_ID, sheet_name= FORMAT_SHEET_NAME, cell= 'B4', update_value= "발행완료")
            print(f"{success_cnt}건 정상 발행 완료")
            return
    
    else :
        print("발행대기 중이 아닙니다.")
        return 
    

with DAG(
    dag_id="insert_ticket_cleaning_people", # dag_id - 보통 파일명과 동일하게 
    schedule= "0 7 * * *", # cron 스케줄
    start_date=pendulum.datetime(2024, 11, 6, 7, 0, tz="Asia/Seoul"), # 이전 스케줄 실행 date와 일치시키자
    catchup=False, # 과거 데이터 소급적용
    tags=["automation", "api", "insert_ticket"], # 태그값
    default_args= {
        'on_failure_callback' : notify_failure
    }
    
) as dag:
    task_cleaning_people = PythonOperator(
        task_id='task_cleaning_people',
        python_callable= insert_ticket_handler
    )
    