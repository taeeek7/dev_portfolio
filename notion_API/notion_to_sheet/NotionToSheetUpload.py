from oauth2client.service_account import ServiceAccountCredentials
from numpyencoder import NumpyEncoder
from  datetime import datetime
import requests, json
import pandas as pd
import numpy as np 
import gspread
import time
import json


## 노션 데이터 구글시트 업로드 함수
# 변수: 업로드할 노션 데이터, jsonkey, 시트아이디, 시트명
def NotionToSheetUpload(df_notion, jsonKey, sheetKey, sheetName) : 

    # 데이터프레임 배열화
    dfArray = np.array(df_notion)
    dfRow = len(df_notion)


    # 인증 정보 로드
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(jsonKey, scope)
    client = gspread.authorize(creds)

    # 구글 스프레드시트 불러오기
    doc = client.open_by_key(sheetKey)
    ws = doc.worksheet(sheetName)
    val = ws.get_all_values()

    # 결과 값을 스프레드시트에 입력
    gsRow = len(val)+1 #시트 마지막행 확인
    ws.add_rows(dfRow) # 빈 행 데이터 준비

    #배열 리스트화 하여 시트 업데이트
    ws.update(f"A{gsRow}", dfArray.tolist())

    #터미널결과출력
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("파이썬실행_고객클레임`24")
    print(now)
    print(f"{dfRow} 건이 업데이트 되었습니다.")



