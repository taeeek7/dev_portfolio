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

# DB추출 및 시트 업로드 함수 정의 (append형) 
# 변수 항목: 실행파일제목, 실행할 쿼리문, json_key, 스프레드시트key, 시트명
def sql_gspread_append(title, query, json_key, sheet_key, ws_name) : 
	
	#접속정보
	conn = pymysql.connect(host='host', 
						user='user_name', 
						password='password', 
						db='db_name', 
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
	empty_rows = sql_row  # 빈 행 추가할 개수
	ws.add_rows(empty_rows) # 빈 행 데이터 준비
	time.sleep(0.5)
	ws.update(f"A{gs_row}" , array.tolist()) #배열 리스트화 하여 시트 업데이트

	#터미널창 결과 입력
	now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	print(f"파이썬실행_{title}")
	print(now)
	print(f"{sql_row}행이 추가되었습니다.")

# DB추출 및 시트 업로드 함수 정의 (reset형_dashboard입력) 
# 변수 항목: 실행파일제목, 실행할 쿼리문, json_key, 스프레드시트key, 시트명1, 시트명2, 기준일시입력셀, 결과입력셀
def sql_gspread_reset(title, query, json_key, sheet_key, ws_name, ws_name_2, standard_date, result_insert) : 
	
	#접속정보
	conn = pymysql.connect(host='host', 
						user='user_name', 
						password='password', 
						db='db_name', 
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

	# 인증 정보 로드
	scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
	creds = ServiceAccountCredentials.from_json_keyfile_name(json_key, scope)
	client = gspread.authorize(creds)

	# 구글 스프레드시트 불러오기
	doc = client.open_by_key(sheet_key)
	ws = doc.worksheet(ws_name)
	ws_2 = doc.worksheet(ws_name_2)
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

	# sql 결과문 업데이트 
	ws.update("A2" , array.tolist()) #배열 리스트화 하여 시트 업데이트

	#대시보드 결과 입력
	now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	ws_2.update(standard_date,now)
	ws_2.update(result_insert,f"{sql_row}행이 추가되었습니다.")

	#터미널창 결과 입력
	print(now)
	print(f"파이썬실행_{title}")
	print(f"{sql_row}행이 추가되었습니다.")



# DB추출 및 시트 업로드 함수 정의 (reset형_dashboard없음)
# 변수 항목: 실행파일제목, 실행할 쿼리문, json_key, 스프레드시트key, 시트명1
def sql_gspread_reset_v2(title, query, json_key, sheet_key, ws_name) : 
	
	#접속정보
	conn = pymysql.connect(host='host', 
						user='user_name', 
						password='password', 
						db='db_name', 
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

	# 인증 정보 로드
	scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
	creds = ServiceAccountCredentials.from_json_keyfile_name(json_key, scope)
	client = gspread.authorize(creds)

	# 구글 스프레드시트 불러오기
	doc = client.open_by_key(sheet_key)
	ws = doc.worksheet(ws_name)
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
	ws.update("A2" , array.tolist()) #배열 리스트화 하여 시트 업데이트

	#터미널창 결과 입력
	now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	print(now)
	print(f"파이썬실행_{title}")
	print(f"{sql_row}행이 추가되었습니다.")

