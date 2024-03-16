from numpyencoder import NumpyEncoder
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import time
import gspread
import numpy as np 
import pymysql
import pandas as pd

#전역변수 선언부
conn = None
cur = None
sql=""

### int형 컬럼 업데이트 기능 추가 __ ver.2 ###


### DB추출 및 시트 업로드 함수 정의 (append형)
### 변수 항목: 
# 실행파일제목, 실행할 쿼리문, json_key, 스프레드시트key, 시트명1, int로 변환할 컬럼 (형식: [1,2,3]) , int형으로 업데이트 시작할 셀 (알파벳만)

def sql_gspread_convert_int_append(title, query, jsonKey, sheetKey, wsName, convertIntCol, updateIntCell) : 
	
	#접속정보
	conn = pymysql.connect(host="host", 
						user="user", 
						password="password", 
						db="db", 
						charset='utf8')
	
	#실행할 sql 구문
	sql= query

	#커서생성
	cur = conn.cursor()

	# cursor 객체를 이용해서 수행
	cur.execute(query)

	# select 된 결과 셋 얻어오기
	result = cur.fetchall()  # tuple 이 들어있는 list

	sql_row = len(result)
	array = np.array(result)

	conn.commit()
	conn.close()

	# 특정 열들을 선택하고 모든 요소를 int로 변환
	convertIntCol = convertIntCol
	arrayInt = array[:, convertIntCol].astype(int)
	
	# 인증 정보 로드
	scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
	creds = ServiceAccountCredentials.from_json_keyfile_name(jsonKey, scope)
	client = gspread.authorize(creds)

	# 구글 스프레드시트 불러오기
	doc = client.open_by_key(sheetKey)
	ws = doc.worksheet(wsName)
	val = ws.get_all_values()

	# 결과 값을 스프레드시트에 입력
	
	gs_row = len(val)+1  #시트 마지막행 확인
	empty_rows = sql_row  # 빈 행 추가할 개수
	ws.add_rows(empty_rows) # 빈 행 데이터 준비
	time.sleep(0.5)
	
	#String형식
	ws.update(f"A{gs_row}" , array.tolist()) #배열 리스트화 하여 시트 업데이트
	
	#int형식
	ws.update(f"{updateIntCell}{gs_row}" , arrayInt.tolist()) #배열 리스트화 하여 시트 업데이트
	
    #터미널창 결과 입력
	now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	print(now)
	print(f"파이썬실행_{title}")
	print(f"{sql_row}행이 추가되었습니다.")



### DB추출 및 시트 업로드 함수 정의 (reset형)
### 변수 항목: 
# 실행파일제목, 실행할 쿼리문, json_key, 스프레드시트key, 시트명1, int로 변환할 컬럼 (형식: [1,2,3]) , int형으로 업데이트 시작할 셀 (알파벳만)

def sql_gspread_convert_int_reset(title, query, jsonKey, sheetKey, wsName, convertIntCol, updateIntCell) : 
	
	#접속정보
	conn = pymysql.connect(host="host", 
						user="user", 
						password="password", 
						db="db", 
						charset='utf8')
	
	#실행할 sql 구문
	sql= query

	#커서생성
	cur = conn.cursor()

	# cursor 객체를 이용해서 수행
	cur.execute(query)

	# select 된 결과 셋 얻어오기
	result = cur.fetchall()  # tuple 이 들어있는 list

	sql_row = len(result)
	array = np.array(result)

	conn.commit()
	conn.close()

	# 특정 열들을 선택하고 모든 요소를 int로 변환
	convertIntCol = convertIntCol
	arrayInt = array[:, convertIntCol].astype(int)
	
	# 인증 정보 로드
	scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
	creds = ServiceAccountCredentials.from_json_keyfile_name(jsonKey, scope)
	client = gspread.authorize(creds)

	# 구글 스프레드시트 불러오기
	doc = client.open_by_key(sheetKey)
	ws = doc.worksheet(wsName)
	val = ws.get_all_values()
	gs_row = len(val)

	#시트 기존 값 지우기 _ ###231126 업데이트 사항: 원본 시트 row 행 1 이하일 경우 삭제 절차 없음.  
	if gs_row >= 2 : 
		ws.delete_rows(2,gs_row)
		ws.append_row([""])
		time.sleep(0.5)
	else : 
		ws.append_row([""])
		time.sleep(0.5)

	# 결과 값을 스프레드시트에 입력
	
	#String형식
	ws.update("A2" , array.tolist()) #배열 리스트화 하여 시트 업데이트
	
	#int형식
	ws.update(f"{updateIntCell}2" , arrayInt.tolist()) #배열 리스트화 하여 시트 업데이트

	#터미널창 결과 입력
	now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	print(now)
	print(f"파이썬실행_{title}")
	print(f"{sql_row}행이 업데이트 되었습니다.")