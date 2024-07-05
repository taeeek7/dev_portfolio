from slack_bolt import App # type: ignore
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk import WebClient
from oauth2client.service_account import ServiceAccountCredentials
from template_format import title_format, result_format, basic_format, certification_format, attachment_format, recruit_format, result_alert_format
from datetime import datetime
import gspread
import json, sys, os, time
import pandas as pd
import numpy as np
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from common_module.SlackBot import SlackBot

file_path_local = "/Users/tin/python-pj/json_file/"
file_path_ubuntu = "/home/ubuntu/json_file/"

json_load = json.load(open(file= file_path_ubuntu+"slack_api.json", encoding = "utf-8"))
json_manager = json.load(open(file= file_path_ubuntu+"manager.json", encoding = "utf-8"))

slack_token = json_load["token"]["requestBot"]
app_token = json_load["App_token"]["requestBot"]
slack_message_channel = "C073S4Q8TNG"     #json_load["channel"]["test2"]  ### 운영_11c-열한시gpt C073S4Q8TNG
slack_error_token = json_load["token"]["11c_bot"]
slack_error_channel = json_load["channel"]["error"]

# 앱 호출
app = App(token= slack_token)

# 알림봇 인사~  
@app.message("안녕 요청봇")
def message_hello(message, say) :
    if message["text"] == "안녕 요청봇" :
        say("어쩌라고?!👻")
    else : 
        pass

# 기타 메시지 무시 함수 
@app.event("message")
def handle_message_events():
    pass

### process 1: 슬래시 커맨드 핸들러
@app.command("/요청사항접수")
def open_modal(ack, body, client):
    ack()

    try : 
        # 모달 트리거
        client.views_open(
            trigger_id= body["trigger_id"]
            ,view= title_format(root_trigger_id= body["trigger_id"])
        )
    except Exception as e :
            crontab_error = SlackBot(slack_error_token, slack_error_channel)
            crontab_error.send_messages(text= f"*🤬 슬랙봇 RequestBot 오류 알림*\n\n   ● 오류내용 : {e}\n")

### process 1-1: 슬래시 커맨드 핸들러 - 가이드 버전
@app.command("/가이드요청봇")
def send_guide(ack, say):
    ack()
    text =  ( f"📬 *요청사항접수 슬랙봇 사용 가이드*\n\n"
        +f"  1. 슬랙 메시지 입력창에 `/요청사항접수` 입력 후 전송\n"
        +f"  2. 접수창이 뜨면 대표 카테고리 선택\n"
        +f"  3. 상세 요청 사항을 항목에 맞게 작성 후 제출\n"
        +f"  4. `#운영_11c-열한시gpt` 채널에 접수된 내용 확인\n\n"
        +f"※ 접수 건이 해결되면 스레드에서 결과를 확인하실 수 있습니다.\n"
        +f"※ 가이드를 다시 보시려면 슬랙 메시지 입력창에 `/가이드요청봇`를 입력해 주세요."
    )
    say(text)
    
### process 2: 요청사항 상세 내용 작성 모달 - 카테고리별로 상세 내용 상이
@app.view("modal_request_form")
def handle_form_modal(ack, body, client):
    ack()
    
    try : 
        today = datetime.now().strftime("%Y-%m-%d")

        # 선택한 카테고리 파싱
        root_trigger_id = body["view"]["private_metadata"]
        category = body["view"]["state"]["values"]["select_request_category"]["select-action"]["selected_option"]["text"]["text"]

        if (
            category == '객실 추가 / 객실 정보 수정 / 단가 수정'
            or category == '린넨 업무'
            or category == '세탁 업체 협의 사항'
            or category == '청소용품 업무'
            or category == '카트 업무'
            or category == '포인트 정책(추가/차감)'
            or category == '현장 문제 해결'
        ) :  
            client.views_open(
                trigger_id= body["trigger_id"]
                ,view= basic_format(
                    root_trigger_id= root_trigger_id
                    ,title= category
                    ,request_content_name= "요청 내용"
                    ,today= today
                    )
            )
        elif (
            category == '고객사(핸디즈 외) 협의 사항'
            or category == '핸디즈 협의 사항'
        ) :
            client.views_open(
                trigger_id= body["trigger_id"]
                ,view= basic_format(
                    root_trigger_id= root_trigger_id
                    ,title= category
                    ,request_content_name= "협의 요청 사항"
                    ,today= today
                    )
            )
        elif (
            category == '업무자동화 오류 수정'
            or category == '지점 카카오 채널 가이드'
        ) :
            client.views_open(
                trigger_id= body["trigger_id"]
                ,view= attachment_format(
                    root_trigger_id= root_trigger_id
                    ,title= category
                    ,today= today
                    )
            )
        elif (
            category == '증명서 요청'
        ) : 
            client.views_open(
                trigger_id= body["trigger_id"]
                ,view= certification_format(
                    root_trigger_id= root_trigger_id
                    ,title= category
                    ,today= today
                    )
            )
        elif (
            category == '키퍼 모집'
        ) : 
            client.views_open(
                trigger_id= body["trigger_id"]
                ,view= recruit_format(
                    root_trigger_id= root_trigger_id
                    ,title= category
                    ,today= today
                    )
            )
        elif (
            category == '(플랫폼) 관제페이지-kcms 오류 제보'
            or category == '(플랫폼) 키퍼앱 오류 제보'
        ) : 
            client.views_open(
                trigger_id= body["trigger_id"]
                ,view= basic_format(
                    root_trigger_id= root_trigger_id
                    ,title= category
                    ,request_content_name= "오류 내용 - 관련 사진을 스레드에 첨부해주세요"
                    ,today= today
                    )
            )
        else : 
            pass 
    except Exception as e :
            crontab_error = SlackBot(slack_error_token, slack_error_channel)
            crontab_error.send_messages(text= f"*🤬 슬랙봇 RequestBot 오류 알림*\n\n   ● 오류내용 : {e}\n")

### process 3-1: 기본 요청 form 슬랙 전송 및 구글 시트 적재
@app.view("basic_data")
def handle_basic_data(ack, body) : 
    ack()
    
    try : 
        # 제출 데이터 파싱
        root_trigger_id = body["view"]["private_metadata"]
        category = body["view"]["title"]["text"]
        username = body["user"]["username"]
        date = body["view"]["state"]["values"]["date_block"]["datepicker-action"]["selected_date"]
        branch = body["view"]["state"]["values"]["branch_block"]["plain_text_input-action"]["value"]
        content = body["view"]["state"]["values"]["content_block"]["plain_text_input-action"]["value"]
        content_title = body["view"]["blocks"][2]["label"]["text"]
        manager_group = json_manager["responsibility"][f"{category}"]["manager_name"]
        manager_name1, manager_name2 = manager_group.split(',')

        # 슬랙봇 전송
        response = WebClient(token= slack_token
                    ).chat_postMessage(
                        channel= slack_message_channel
                        ,text= "운영사 요청 사항 접수"
                        ,blocks= [
                            {
                                "type": "section",
                                "text": {
                                    "type": "mrkdwn",
                                    "text": "*_📝 운영사 요청 사항 접수_*"
                                }
                            },
                            {
                                "type": "section",
                                "text": {
                                    "type": "mrkdwn",
                                    "text":  f">카테고리: {category}\n"
                                            +f">담당자: <@{manager_name1}>, <@{manager_name2}>"
                                }
                            },
                            {
                                "type": "divider"
                            },
                            {
                                "type": "section",
                                "text": {
                                    "type": "mrkdwn",
                                    "text":  f">요청자: <@{username}>\n"
                                            +f">요청 지점: {branch}\n"
                                            +f">{content_title}: \n\n" 
                                            +f">{content}"
                                }
                            },
                            {
                                "type": "context",
                                "elements": [
                                    {
                                        "type": "plain_text",
                                        "text": f"{root_trigger_id}",
                                        "emoji": True
                                    }
                                ]
                            }
                        ]
                    )   
        # 전송 메시지 타임스탬프 획득
        original_ts = response['ts'] 

        list = [[category, manager_name1, date, username, branch, content, '', '', '', '', '', '', root_trigger_id, original_ts]]

        # gspread 변수 설정
        json_key = "/home/ubuntu/service_key/keeper-data-4c16ed1166b5.json"
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

        # 결과 값을 스프레드시트에 입력
        gs_row = len(val)+1  #시트 마지막행 확인
        ws.add_rows(1) # 빈 행 데이터 준비
        time.sleep(0.5)
        ws.update(f"B{gs_row}", list)
    
    except Exception as e :
            crontab_error = SlackBot(slack_error_token, slack_error_channel)
            crontab_error.send_messages(text= f"*🤬 슬랙봇 RequestBot 오류 알림*\n\n   ● 오류내용 : {e}\n")

### process 3-2: 첨부링크 요청 form 슬랙 전송 및 구글 시트 적재
@app.view("attachment_data")
def handle_attachment_data(ack, body) :
    ack()

    try :
        # 제출 데이터 파싱
        root_trigger_id = body["view"]["private_metadata"]
        category = body["view"]["title"]["text"]
        username = body["user"]["username"]
        date = body["view"]["state"]["values"]["date_block"]["datepicker-action"]["selected_date"]
        branch = body["view"]["state"]["values"]["branch_block"]["plain_text_input-action"]["value"]
        content = body["view"]["state"]["values"]["content_block"]["plain_text_input-action"]["value"]
        link = body["view"]["state"]["values"]["link_block"]["plain_text_input-action"]["value"]
        manager_name = json_manager["responsibility"][f"{category}"]["manager_name"]

        response = WebClient(token= slack_token
                    ).chat_postMessage(
                        channel= slack_message_channel
                        ,text= "운영사 요청 사항 접수"
                        ,blocks= [
                            {
                                "type": "section",
                                "text": {
                                    "type": "mrkdwn",
                                    "text": "*_📝 운영사 요청 사항 접수_*"
                                }
                            },
                            {
                                "type": "section",
                                "text": {
                                    "type": "mrkdwn",
                                    "text":  f">카테고리: {category}\n"
                                            +f">담당자: <@{manager_name}>"
                                }
                            },
                            {
                                "type": "divider"
                            },
                            {
                                "type": "section",
                                "text": {
                                    "type": "mrkdwn",
                                    "text":  f">요청자: <@{username}>\n"
                                            +f">요청 지점: {branch}\n"
                                            +f">요청 내용: {content}\n"
                                            +f">관련 링크: {link}\n"
                                }
                            },
                            {
                                "type": "context",
                                "elements": [
                                    {
                                        "type": "plain_text",
                                        "text": f"{root_trigger_id}",
                                        "emoji": True
                                    }
                                ]
                            }
                        ]
                    )
        # 전송 메시지 타임스탬프 획득
        original_ts = response['ts'] 

        list = [[category, manager_name, date, username, branch, content, link, '', '', '', '', '', root_trigger_id, original_ts]]

        # gspread 변수 설정
        json_key = "/home/ubuntu/service_key/keeper-data-4c16ed1166b5.json"
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

        # 결과 값을 스프레드시트에 입력
        gs_row = len(val)+1  #시트 마지막행 확인
        ws.add_rows(1) # 빈 행 데이터 준비
        time.sleep(0.5)
        ws.update(f"B{gs_row}", list)
    
    except Exception as e :
            crontab_error = SlackBot(slack_error_token, slack_error_channel)
            crontab_error.send_messages(text= f"*🤬 슬랙봇 RequestBot 오류 알림*\n\n   ● 오류내용 : {e}\n")

### process 3-3: 증명서 요청 form 슬랙 전송 및 구글 시트 적재
@app.view("certification_data")
def handle_certification_data(ack, body) : 
    ack()

    try:
        # 제출 데이터 파싱
        root_trigger_id = body["view"]["private_metadata"]
        category = body["view"]["title"]["text"]
        username = body["user"]["username"]
        date = body["view"]["state"]["values"]["date_block"]["datepicker-action"]["selected_date"]
        branch = body["view"]["state"]["values"]["branch_block"]["plain_text_input-action"]["value"]
        certification_type = body["view"]["state"]["values"]["type_block"]["static_select-action"]["selected_option"]["text"]["text"]
        keeper_name = body["view"]["state"]["values"]["name_block"]["plain_text_input-action"]["value"]
        end_date = body["view"]["state"]["values"]["date_block"]["datepicker-action"]["selected_date"]
        use_case = body["view"]["state"]["values"]["use_block"]["plain_text_input-action"]["value"]
        manager_name = json_manager["responsibility"][f"{category}"]["manager_name"]

        response = WebClient(token= slack_token
            ).chat_postMessage(
                channel= slack_message_channel
                ,text= "운영사 요청 사항 접수"
                ,blocks= [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "*_📝 운영사 요청 사항 접수_*"
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text":  f">카테고리: {category}\n"
                                    +f">담당자: <@{manager_name}>"
                        }
                    },
                    {
                        "type": "divider"
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text":  f">요청자: <@{username}>\n"
                                    +f">요청 지점: {branch}\n"
                                    +f">증명서 종류: {certification_type}\n"
                                    +f">키퍼 이름: {keeper_name}\n"
                                    +f">업무 종료년월일: {end_date}\n"
                                    +f">증명서 용도: {use_case}\n"
                        }
                    },
                    {
                        "type": "context",
                        "elements": [
                            {
                                "type": "plain_text",
                                "text": f"{root_trigger_id}",
                                "emoji": True
                            }
                        ]
                    }
                ]
            )   
        # 전송 메시지 타임스탬프 획득
        original_ts = response['ts'] 

        list = [[category, manager_name, date, username, branch, certification_type, '', '', '', keeper_name, end_date, use_case, root_trigger_id, original_ts]]

        # gspread 변수 설정
        json_key = "/home/ubuntu/service_key/keeper-data-4c16ed1166b5.json"
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

        # 결과 값을 스프레드시트에 입력
        gs_row = len(val)+1  #시트 마지막행 확인
        ws.add_rows(1) # 빈 행 데이터 준비
        time.sleep(0.5)
        ws.update(f"B{gs_row}", list)

    except Exception as e :
            crontab_error = SlackBot(slack_error_token, slack_error_channel)
            crontab_error.send_messages(text= f"*🤬 슬랙봇 RequestBot 오류 알림*\n\n   ● 오류내용 : {e}\n")

### process 3-4: 키퍼 모집 요청 form 슬랙 전송 및 구글 시트 적재
@app.view("recruit_data")
def handle_recruit_data(ack, body) :
    ack()

    try:
        # 제출 데이터 파싱
        root_trigger_id = body["view"]["private_metadata"]
        category = body["view"]["title"]["text"]
        username = body["user"]["username"]
        date = body["view"]["state"]["values"]["date_block"]["datepicker-action"]["selected_date"]
        branch = body["view"]["state"]["values"]["branch_block"]["plain_text_input-action"]["value"]
        recruit_type = body["view"]["state"]["values"]["type_block"]["static_select-action"]["selected_option"]["text"]["text"]
        ads_period = body["view"]["state"]["values"]["ads_block"]["plain_text_input-action"]["value"]
        manager_name = json_manager["responsibility"][f"{category}"]["manager_name"]

        # 필요 요일 체크 액션 찾기
        def week_parsing() : 
            week_cnt = len(body["view"]["state"]["values"]["week_block"]["checkboxes-action"]["selected_options"])
            week_list = []

            for i in range(0, week_cnt) : 
                check_week = body["view"]["state"]["values"]["week_block"]["checkboxes-action"]["selected_options"][i]["text"]["text"]
                week_list.append(check_week)
            
            join_str = ','.join(week_list)

            return join_str
        
        response = WebClient(token= slack_token
            ).chat_postMessage(
                channel= slack_message_channel
                ,text= "운영사 요청 사항 접수"
                ,blocks= [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "*_📝 운영사 요청 사항 접수_*"
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text":  f">카테고리: {category}\n"
                                    +f">담당자: <@{manager_name}>"
                        }
                    },
                    {
                        "type": "divider"
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text":  f">요청자: <@{username}>\n"
                                    +f">모집 지점: {branch}\n"
                                    +f">요청 포지션: {recruit_type}\n"
                                    +f">필요 요일: {week_parsing()}\n"
                                    +f">유료공고 기간: {ads_period}\n"
                        }
                    },
                    {
                        "type": "context",
                        "elements": [
                            {
                                "type": "plain_text",
                                "text": f"{root_trigger_id}",
                                "emoji": True
                            }
                        ]
                    }
                ]
            )
        # 전송 메시지 타임스탬프 획득
        original_ts = response['ts'] 

        list = [[category, manager_name, date, username, branch, recruit_type, '', week_parsing(), ads_period, '', '', '', root_trigger_id, original_ts]]

        # gspread 변수 설정
        json_key = "/home/ubuntu/service_key/keeper-data-4c16ed1166b5.json"
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

        # 결과 값을 스프레드시트에 입력
        gs_row = len(val)+1  #시트 마지막행 확인
        ws.add_rows(1) # 빈 행 데이터 준비
        time.sleep(0.5)
        ws.update(f"B{gs_row}", list)
    
    except Exception as e :
            crontab_error = SlackBot(slack_error_token, slack_error_channel)
            crontab_error.send_messages(text= f"*🤬 슬랙봇 RequestBot 오류 알림*\n\n   ● 오류내용 : {e}\n")

### process 4: 결과 작성 리액션 이벤트
@app.event("reaction_added")
def handle_reaction(ack, event, client) :
    ack()

    try : 
        # 이벤트 데이터에서 필요한 정보 추출
        reaction = event["reaction"]
        channel_id = event["item"]["channel"]
        thread_ts = event["item"]["ts"]

        # 메시지 본문 가져오기
        response = client.conversations_history(
            channel=channel_id,
            latest=thread_ts,
            limit=1,
            inclusive=True
        )

        # 원 메시지에서 root_trigger_id 찾기
        def root_trigger_id_parsing() : 
            blocks = response["messages"][0]["blocks"]

            for index, block in enumerate(blocks) :
                if block["type"] == "context" : 
                    root_trigger_id = response["messages"][0]["blocks"][index]["elements"][0]["text"]
            
            return root_trigger_id
                
        if reaction == "완료_2" :
            # 메시지에 버튼 추가
            WebClient(token= slack_token
            ).chat_postMessage(
                channel= channel_id
                ,thread_ts= thread_ts
                ,attachments= [
                    {
                        "text": "결과를 작성하려면 버튼을 누르세요.",
                        "fallback": f"{root_trigger_id_parsing()}",
                        "callback_id": "result_modal",
                        "color": "#6ed3b3",
                        "attachment_type": "default",
                        "actions": [
                            {
                                "name": "open_modal",
                                "text": "결과 작성",
                                "type": "button",
                                "value": "open_modal"
                            }
                        ]
                    }
                ]
            )

    except Exception as e :
        crontab_error = SlackBot(slack_error_token, slack_error_channel)
        crontab_error.send_messages(text= f"*🤬 슬랙봇 RequestBot 오류 알림*\n\n   ● 오류내용 : {e}\n")

### process 5: 결과 작성 모달창 오픈
@app.action("result_modal")
def handle_result_modal(ack, body, client) :
    ack()
    ## 결과 모달창 작성 권한 확인 함수
    def user_check(user_name, staff_list) :      
        return user_name in staff_list
    
    # 변수 설정
    user_name = body["user"]["name"] 
    staff_list = json_manager["all_staff"].split(", ")
    today = datetime.now().strftime("%Y-%m-%d")
    trigger_id = body["trigger_id"]
    channel_id = body["channel"]["id"]
    thread_ts = body["original_message"]["ts"]
    root_trigger_id = body["original_message"]["attachments"][0]["fallback"]
    
    # 작성 유저 체크 후 모달창 오픈
    if user_check(user_name= user_name, staff_list= staff_list) == True : 
        try :
            # 모달 트리거
            client.views_open(
                trigger_id= trigger_id
                ,view= result_format(channel_id, thread_ts, root_trigger_id, today)
            )

        except Exception as e :
                crontab_error = SlackBot(slack_error_token, slack_error_channel)
                crontab_error.send_messages(text= f"*🤬 슬랙봇 RequestBot 오류 알림*\n\n   ● 오류내용 : {e}\n")
    else :
        # 모달 트리거
            client.views_open(
                trigger_id= trigger_id
                ,view= result_alert_format()
            )

### process 5-1: 권한 경고 모달창 닫기
@app.view("alert_close")
def handle_alert_modal(ack) :
    ack()
    return 

### process 6: 결과 작성 내용 스레드 전송 및 구글 시트 적재
@app.view("result_data")
def handle_result_inform(ack, body, client) : 
    ack()
    
    try : 
        # 제출 데이터 파싱
        private_metadata = body["view"]["private_metadata"]
        #user = body["user"]["id"]
        date = body["view"]["state"]["values"]["date_block"]["datepicker-action"]["selected_date"]
        contents = body["view"]["state"]["values"]["content_block"]["plain_text_input-action"]["value"]

        # 모달이 생성된 메시지의 channel_id와 thread_ts 가져오기 (private_metadata)
        channel_id, thread_ts, root_trigger_id = private_metadata.split(',')
        
        # 구글 시트 적재 데이터 원본 
        df_result = pd.DataFrame({
        "root_trigger_id" : [f"{root_trigger_id}"]
        ,"response_date" : [f"{date}"]
        ,"contents" : [f"{contents}"]
        })
        
        # gspread 변수 설정
        json_key = "/home/ubuntu/service_key/keeper-data-4c16ed1166b5.json"
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

        # 루트 트리거 id를 구글 시트에서 찾기
        df_ws = pd.DataFrame(val)
        df_ws['root_trigger_id'] = df_ws[13]  # root_trigger_id 적재 컬럼 >> N열
        df_ws['request_user'] = df_ws[4] # 요청자
        df_ws['row'] = df_ws.index + 1  # 인덱스 0부터 시작하므로 구글 시트와 맞춰주기 위해 +1
        df_root = df_ws[['row', 'root_trigger_id', 'request_user']]

        # 결과 작성 내용과 병합
        df_merge = pd.merge(df_root, df_result, how= 'inner', on= 'root_trigger_id')
        df_upload = df_merge[['response_date', 'contents']]
        array = np.array(df_upload)
        df_row = df_merge.iloc[0,0]

        # 요청자 추출
        send_request_user = df_merge.iloc[0,2]

        # 작성 내용 스레드 발송
        WebClient(token= slack_token
        ).chat_postMessage(
            channel=channel_id,
            thread_ts=thread_ts,  # 스레드에 답글로 작성
            text= 
                f"<@{send_request_user}>, 접수하신 건의 답변 내용입니다.\n\n"
                +f">{contents}"
        )

        # 병합된 데이터를 기준 트리거 id가 있는 스프레드시트 행에 입력
        ws.update(f"P{df_row}", array.tolist())

    except Exception as e :
            crontab_error = SlackBot(slack_error_token, slack_error_channel)
            crontab_error.send_messages(text= f"*🤬 슬랙봇 RequestBot 오류 알림*\n\n   ● 오류내용 : {e}\n")

# Socket 실행 main 함수
if __name__ == "__main__" : 
    SocketModeHandler(app, app_token).start()

