from oauth2client.service_account import ServiceAccountCredentials
from numpyencoder import NumpyEncoder
from  datetime import datetime
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import requests, json
import pandas as pd
import numpy as np 
import gspread
import time
import json

# 노션 데이터베이스 데이터 추가
# 변수항목: 노션데이터베이스id, json_key, 스프레드시트key, 시트명, 발송결과알림 슬랙채널

def upload_database(databaseid,json_key,spreadsheet_key, sheet_name, slack_channel):

    ### 구글 스프레드시트 데이터 불러오기 ###

    # 인증 정보 로드
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_key, scope)
    client = gspread.authorize(creds)

    # 구글 스프레드시트 불러오기
    doc = client.open_by_key(spreadsheet_key)
    ws = doc.worksheet(sheet_name)
    df = pd.DataFrame(ws.get_all_values())
    last_row = len(df)

    #반복문 초기 변수 지정 
    response_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    response_day_before = df.loc[0,11]
    response_day = df.loc[0,12]
    i = 0 
    upload_cnt = 0 

    # DB 업데이트 반복문
    if last_row == 1 : 
        print(response_date)
        print("파이썬실행_객실타입단가_노션업로드")
        print("업데이트 건이 없습니다.")

    else : 
        for i in range(1,last_row) : 
            
            #DB 업데이트 항목 변수 설정 
            clcm = df.loc[i,1]
            branch = df.loc[i,2]
            roomtype = df.loc[i,4]
            price = df.loc[i,6]

            ### API POST 필수 파라미터 및 Token 입력 ###
            token = "token"
            notion_url = "https://api.notion.com/v1/pages"
            headers = {
                "Authorization": "Bearer " + token,
                "Content-Type" : "application/json",
                "Notion-Version": "2022-02-22"
            }

            body = {
                'parent' : {
                    'type' : 'database_id' , 
                    'database_id' : databaseid
                },
                'properties' : {
                    '클라이언트' : {
                        'title' : [ {
                            'text' : {
                                'content' : clcm
                            } 
                        } 
                        ]
                    },
                    '지점명' : {
                        'select' : {
                            'name' : branch
                        }
                    },
                    '객실타입' : {
                        'rich_text' : [ {
                            'text' : {
                                'content' : roomtype
                            }
                        } 
                        ]
                    },
                    '수행비용' : {
                        'type' : 'number',
                        'number' : int(price) 
                    }
                }
            }

            ## API 호출 ###
            req = requests.post(url=notion_url, headers=headers, data=json.dumps(body))
            #print(req.status_code)
            upload_cnt = upload_cnt + 1 
        


        #슬랙 메시지 발송 변수 설정
        slack_token = "slack_token"
        client = WebClient(token=slack_token)


        ### 업데이트 결과 슬랙메시지 발송 ###
        response_slack = client.chat_postMessage(
        channel= slack_channel,      # 채널 id를 입력합니다.
        text=     f"💰 객실타입별 단가 정보 Notion 업로드 결과\n\n" 
                + f"   ● 업로드 일시 : {response_date}\n"
                + f"   ● 업로드 기간 : {response_day_before} ~ {response_day}\n"
                + f"   ● 업로드 건수 : {upload_cnt} 건\n"
                )

        # 터미널창 결과 입력
        print(response_date)
        print("파이썬실행_객실타입단가_노션업로드")
        print(f"{upload_cnt} 건이 업데이트 되었습니다.")