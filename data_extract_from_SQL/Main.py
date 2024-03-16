from SqlGspread import sql_gspread_convert_int_reset, sql_gspread_convert_int_append
from SlackBot import send_slackbot

### 손익지표 활용 데이터 추출 ###

# 1. 수행내역 데이터 -- 일반 ##

try : 
    sql_gspread_convert_int_append(
    "수행내역 데이터 -- 일반"
    ,"""
    ### sql 쿼리문 입력 ###   
    """
    , "google_sheet_key" ## 구글 API 키 입력 
    ,"google_sheet_id"
    ,"sheet_name"
    ,[7,8,9,10,11,12,13]
    ,"H"
    )

except Exception as e : 
    send_slackbot("slack_key" ## slack_API key 입력 
                ,"channel_id"
                ,f"*🤬 파이썬실행_수행내역 데이터 -- 일반 오류 알림*\n\n"
                +f"   ● 오류내용 : {e}\n"
            )