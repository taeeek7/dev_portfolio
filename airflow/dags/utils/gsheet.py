from airflow.providers.google.suite.operators.sheets import GSheetsHook
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd 
import gspread
import decimal

class GsheetUtils :
    def decimal_default(obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        raise TypeError
    
    # 구글시트 append data
    def append_data(sheet_id, sheet_name, values) :
        #객체 생성
        hook = GSheetsHook(gcp_conn_id="google-api")
        
        # Google Sheets에 데이터 업데이트
        hook.append_values(
            spreadsheet_id= sheet_id,
            range_= sheet_name,  # 데이터가 삽입될 셀 범위 지정
            values= values,  # 데이터 전달
            value_input_option='RAW'  # 데이터 입력 방식 설정
        )
    
    # 구글 시트 읽어오기
    def read_gsheet(sheet_key, sheet_id, sheet_name) :
        # 인증 정보 로드
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(sheet_key, scope)
        client = gspread.authorize(creds)

        # 구글 스프레드시트 불러오기
        doc = client.open_by_key(sheet_id)
        ws = doc.worksheet(sheet_name)
        data = pd.DataFrame(ws.get_all_values())

        return data.loc[1:].reset_index(drop= True)
    
    # 구글 시트 셀 읽어오기
    def read_cell_point(sheet_key, sheet_id, sheet_name, cell) :
        # 인증 정보 로드
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(sheet_key, scope)
        client = gspread.authorize(creds)

        # 구글 스프레드시트 불러오기
        doc = client.open_by_key(sheet_id)
        ws = doc.worksheet(sheet_name)
        data = pd.DataFrame(ws.get(cell))
        value = data.loc[0,0]

        return value 
    
    # 구글 시트 셀 업데이트
    def update_cell_point(sheet_key, sheet_id, sheet_name, cell, update_value) :
        # 인증 정보 로드
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(sheet_key, scope)
        client = gspread.authorize(creds)

        # 구글 스프레드시트 불러오기
        doc = client.open_by_key(sheet_id)
        ws = doc.worksheet(sheet_name)
        ws.update(cell, update_value)
 
        return f"{update_value}, 업데이트 완료"

