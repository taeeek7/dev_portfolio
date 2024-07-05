from slack_sdk import WebClient
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import gspread
import pandas as pd 
import json, os, sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from common_module.SlackBot import SlackBot

file_path_local = "/Users/tin/python-pj/json_file/"
file_path_ubuntu = "/home/ubuntu/json_file/"

json_load = json.load(open(file= file_path_ubuntu+"slack_api.json", encoding = "utf-8"))
slack_token = json_load["token"]["requestBot"]
slack_channel = 'C073S4Q8TNG'     #운영_11c-열한시gpt 
slack_error_token = json_load["token"]["11c_bot"]
slack_error_channel = json_load["channel"]["error"]

# 접수건 미완료 7일 경과 리스트 가져오기 함수
def uncompleted_list() : 
    # gspread 변수 설정
    json_key = "/home/ubuntu/service_key/keeper-data-4c16ed1166b5.json"        #"/Users/tin/SERVICE_KEY/keeper-data-4c16ed1166b5.json"
    sheet_key = "1LQlejTY7GhlACHLxf6jYh1HqS8HJ_yo0smCEdZIZAu8"
    ws_name = "접수내역"

    # 인증 정보 로드
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_key, scope)
    client = gspread.authorize(creds)

    # 구글 스프레드시트 불러오기
    doc = client.open_by_key(sheet_key)
    ws = doc.worksheet(ws_name)
    val = ws.get_all_values()

    # 데이터 가공
    df_ws = pd.DataFrame(val)[[2,3,14,15]]
    df_raw = df_ws.iloc[4:].reset_index()
    df_raw['manager'] = df_raw[2]
    df_raw['request_day'] = pd.to_datetime(df_raw[3])
    df_raw['thread_ts'] = df_raw[14]
    df_raw['result_date'] = df_raw[15]
    df_raw['day_over'] = (datetime.now() - df_raw['request_day']).dt.days

    df_list = df_raw[[
        'manager'
        ,'request_day'
        ,'thread_ts'
        ,'result_date'
        ,'day_over'
    ]]

    # 경고 조건 설정 (결과일 없음 and 요청 후 7일 경과)
    condition = "result_date == '' and day_over >= 7"
    df_q = df_list.query(condition).reset_index()

    return df_q

# 리스트 스레드 발송 함수 
def send_thread() : 
    df = uncompleted_list()
    # 0건 예외처리
    if len(df) == 0 :
        return 
    else :
        try : 
            i = 0 
            for i in range(0, len(df)) :
                manager = df.iloc[i,1]
                thread_ts = df.iloc[i,3]
                day_over = df.iloc[i,5]

                # 작성 내용 스레드 발송
                WebClient(token= slack_token
                ).chat_postMessage(
                    channel=slack_channel,
                    thread_ts=thread_ts,  # 스레드에 답글로 작성
                    text= f"<@{manager}>, 요청 건이 접수일로부터 {day_over}일 경과했습니다.\n\n"
                )
        except Exception as e :
            crontab_error = SlackBot(slack_error_token, slack_error_channel)
            crontab_error.send_messages(text= f"*🤬 슬랙봇 AlertMain 오류 알림*\n\n   ● 오류내용 : {e}\n")

# 실행 main 함수
if __name__ == "__main__" : 
    send_thread()
    #print(uncompleted_list())
