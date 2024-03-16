from numpyencoder import NumpyEncoder
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.alert import Alert
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import time
import requests
import json
import pymysql
import pandas as pd
import numpy as np 


#전역변수 선언부
conn = None
cur = None
sql=""

#접속정보  -- 접속정보 변수 목적에 맞게 변경
conn = pymysql.connect(host='host', 
                       user='username', 
                       password='password', 
                       db='db_name', 
                       charset='utf8')

#커서생성
cur = conn.cursor()

#실행할 sql 구문 
sql=  """  query insert """  ## sql query 문 입력

# cursor 객체를 이용해서 수행
cur.execute(sql)

# select 된 결과 셋 얻어오기
result = cur.fetchall()  # tuple 이 들어있는 list

#sql 접속 종료
conn.commit()
conn.close()

#sql 결과 데이터프레임 및 변수 설정
df = pd.DataFrame(result)
last_row = len(df)
print(df)
print(f"총 {last_row}건")


### 알림톡 API 발송을 위한 변수 설정 ###
i = 0 
result_day = datetime.now().strftime("%d")
now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


### 슬랙 API 및 메시지 발송 변수 설정 ###
slack_token = "slack_token"
client = WebClient(token=slack_token)
success_cnt = 0 
error_cnt = 0 
error_code = []
error_message = []
error_keeper = []


### keeper WEB 로그인 ###

#크롬드라이브 옵션 설정
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('headless')
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

#크롬 페이지 열기
driver.get('webpage_site')

# 로그인
search_box = driver.find_element(By.XPATH, "//*[@id='root']/section/div[2]/div[1]/div[2]/div[1]/input")
search_box.send_keys("id")
search_box = driver.find_element(By.XPATH, "//*[@id='root']/section/div[2]/div[1]/div[2]/div[2]/input")
search_box.send_keys("password")

login_button = driver.find_element(By.XPATH, "//*[@id='root']/section/div[2]/div[1]/div[2]/button")
login_button.click()
time.sleep(2)


# API 발송 반복문 설정 
for i in range(0,last_row) :
    
    ###비즈알림톡 API 변수 설정
    appkey = "appkey"
    secretkey = "secretkey"
    sender_key = "sender_key"
    template_code = "code_name"
    
    ### 비즈알림톡 발신내용 변수 설정
    recipient_no = df.loc[i,3]
    template_parameter = {
        "name": df.loc[i,2] , 
        "branch" : df.loc[i,0] ,
        "ch_url" : df.loc[i,5] ,
        "ytb_url" : "https://m.youtube.com/watch?v=MlMheHn0vJg",  
    }

    # API 엔드포인트 URL
    url = f"https://api-alimtalk.cloud.toast.com/alimtalk/v2.3/appkeys/{appkey}/messages"

    # 요청 헤더 설정
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "X-Secret-Key": secretkey
    }

    # 요청 본문 데이터 설정
    requestBody = {
        "senderKey": sender_key,
        "templateCode": template_code,
        "recipientList": [{
            "recipientNo": recipient_no,
            "templateParameter": template_parameter,
            "resendParameter": {
              "isResend" : True,
              "resendTitle" : "열한시 키퍼",
              "resendSendNo" : "resend_number"
            }
        }]
    }
    
    # POST 요청 보내기
    response = requests.post(url, headers=headers, json=requestBody)
    response_text = json.loads(response.text)
    response_Code = response_text['header']['resultCode']
    response_message = response_text['header']['resultMessage']


    ### 알림톡 발송 후 결과값 입력 ###   
    # 변수 설정
    keeper_id = df.loc[i,1]
    keeper_name = df.loc[i,2]
    
    ### 알림톡 발송 성공 시 ###
    if response_Code == 0 : 

        #계정상세페이지 접속
        driver.get(f'https://kcms.11c.co.kr/account-fulfillments-detail/{keeper_id}/')

        memo = driver.find_element(By.NAME, "targetMemo")
        memo.send_keys(f'{result_day}')

        convert = driver.find_element(By.XPATH, "//*[@id='root']/div/div[1]/div[5]/button[2]")
        convert.click()
        time.sleep(1)
        
        #성공건수 count
        success_cnt = success_cnt + 1
        

    ### 알림톡 발송 실패 시 ###
    else : 
          
        #계정상세페이지 접속
        driver.get(f'https://kcms.11c.co.kr/account-fulfillments-detail/{keeper_id}/')

        memo = driver.find_element(By.NAME, "targetMemo")
        memo.send_keys(f"발송실패 ({response_Code})")

        convert = driver.find_element(By.XPATH, "//*[@id='root']/div/div[1]/div[5]/button[2]")
        convert.click()
        time.sleep(1)

        # 에러건수 count 
        error_cnt = error_cnt + 1 
        error_message.append(response_message)
        error_code.append(response_Code)
        error_keeper.append(keeper_name)


#크롬 종료 
driver.quit()

### API 호출 결과 슬랙메시지 발송 ###
response_slack = client.chat_postMessage(
channel="C05PKAP3PK6",      # 채널 id를 입력합니다.
text=     f"💌 신규키퍼 알림톡 자동발송\n\n" 
        + f"   ● 실행일시 : {now}\n"
        + f"   ● 실행건수 : {last_row} 건\n"
        + f"   ● 결과 : 성공 {success_cnt} 건  / 실패 {error_cnt} 건\n"
        + f"         ○ error_code : {error_code}\n"
        + f"         ○ error_keeper : {error_keeper}"
        )


#터미널창 결과 입력
print("파이썬실행_알림톡발송")
print(now)
print(f"성공 {success_cnt} 건  / 실패 {error_cnt} 건")
print(f"{error_message}")
