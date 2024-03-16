from dev_portfolio.notion_API.notion_to_sheet.ExtractAttachments import ExtractAttachments
from dev_portfolio.notion_API.notion_to_sheet.NotionToSheetUpload import NotionToSheetUpload
from dev_portfolio.notion_API.notion_to_sheet.SlackBot import send_slackbot
from dev_portfolio.notion_API.notion_to_sheet.MergeClaimList import MergeClaimList
from  datetime import datetime
import numpy as np
import pandas as pd


try : 
    ### 실행일자 정의
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    ### 접수일자 정의 (D-1)
    date_sub = datetime.now() - pd.Timedelta(days=1)    #실행일자-1 
    reception_date = date_sub.strftime("%Y-%m-%d")

    ### 접수건수 불러오기
    reception_cnt = len(MergeClaimList("notion_database_id"  ## 노션 데이터베이스 id 입력 
                              ,"secret_key"  ## 노션 API 키 입력
                              ))


    ### 업로드 실행 및 결과 슬랙봇 발송
    NotionToSheetUpload(MergeClaimList("notion_database_id"  ## 노션 데이터베이스 id 입력
                                      ,"secret_key"  ## 노션 API 키 입력
                                      )
                      , "google_sheet_key" ## 구글 API 키 입력 
                      , "notion_database_id"  ## 노션 데이터베이스 id 입력
                      , "upload_sheet_name"
                      )
    send_slackbot("slack_key" ## slack_API key 입력 
                ,"channel_id"
                ,f"⚠️ 고객 클레임 업데이트 결과\n\n" 
                  + f"   ● 실행일시 : {now}\n"
                  + f"   ● 작성일자 : {reception_date}\n"
                  + f"   ● 작성건수 : {reception_cnt} 건\n"
              )

except KeyError :
    print(now)
    print("접수건이 없습니다.")

except Exception as e :
    send_slackbot("slack_key" ## slack_API key 입력 
                ,"channel_id"
                  ,f"*🤬 핸디즈 고객클레임 추출 오류 알림*\n\n"
                  +f"   ● 오류내용 : {e}\n"
                )