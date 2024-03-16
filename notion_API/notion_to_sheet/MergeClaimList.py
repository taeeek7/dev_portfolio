from dev_portfolio.notion_API.notion_to_sheet.ExtractAttachments import ExtractAttachments
from dev_portfolio.notion_API.notion_to_sheet.NotionToSheetUpload import NotionToSheetUpload
from dev_portfolio.notion_API.notion_to_sheet.SlackBot import send_slackbot
from oauth2client.service_account import ServiceAccountCredentials
from numpyencoder import NumpyEncoder
from  datetime import datetime
import requests, json
import pandas as pd
import numpy as np 
import gspread
import time
import json


# 핸디즈 노션 고객클레임 데이터베이스 추출 
# 변수항목: 데이터베이스id
def MergeClaimList(databaseId, notionApiKey):
    
    ### 노션 데이터베이스 ID 및 API Token 변수 입력 ###
    token = notionApiKey
    notion_url = f"https://api.notion.com/v1/databases/{databaseId}/query"
    headers = {
        "Authorization": "Bearer " + token,
        "Notion-Version": "2022-02-22"
    }

    ### API 호출 및 상태 코드 확인 ###
    req = requests.post(url=notion_url, headers=headers)
    data = req.json()

    ###데이터프레임 개요###
    df = pd.json_normalize(data['results'])

    ### 텍스트 추출 함수 정의 ###
    def extract_text(x):
        if x and len(x) > 0:
            return x[0]['text']['content']
        else:
            return None
    
    ### 다중 선택 추출 함수 정의 ###
    def extract_multi_select(x):
        if x and len(x) > 0:
            return x[0]['name']
        else:
            return None

    ### 딕셔너리 value 값 추출 및 컬럼명 변경 ###
    df['pageId'] = df['id']
    df['항목'] = df['properties.항목.select.name']
    df['접수일자'] = df['properties.접수 일자.date.start']
    df['지점명'] = df['properties.지점명.select.name']
    df['객실번호'] = df['properties.객실번호.title'].apply(extract_text)
    df['체크인'] = df['properties.체크인.date.start']
    df['접수 내용(리뷰내용)'] = df['properties.접수 내용(리뷰내용).rich_text'].apply(extract_text)
    df['고객 클레임 링크'] = df['properties.고객 클레임 링크.url']
    df['처리내용'] = df['properties.처리내용.multi_select'].apply(extract_multi_select)
    df['처리내용(세부)'] = df['properties.처리내용(세부).rich_text'].apply(extract_text)
    df['클리닝 일자'] = df['properties.클리닝 일자.date.start']
    df['담당 키퍼'] = df['properties.담당 키퍼.rich_text'].apply(extract_text)
    df['클리닝 구분'] = df['properties.클리닝 구분.select.name']
    df['작성자'] = df['properties.작성자.created_by.id']
    df['작성일자_kst'] = pd.to_datetime(df['created_time']) + pd.Timedelta(hours=9)
    df['작성일'] = df['작성일자_kst'].dt.strftime("%Y-%m-%d")
    

    ### 데이터 추출 조건 설정 ###
    # 1. 접수일자가 실행일자 기준 D-1일
    # 2. 항목: 고객클레임
    date_sub = datetime.now() - pd.Timedelta(days=1)    #실행일자-1 
    date_format = date_sub.strftime("%Y-%m-%d")
    condition = f"작성일 == '{date_format}' and 항목 == '고객클레임' "
    df_q = df.query(condition).reset_index()

    df_index = df_q[['pageId'
                ,'항목'
                ,'접수일자'
                ,'지점명'
                ,'객실번호'
                ,'체크인'
                ,'접수 내용(리뷰내용)'
                ,'고객 클레임 링크'
                ,'처리내용'
                ,'처리내용(세부)'
                ,'클리닝 일자'
                ,'담당 키퍼'
                ,'클리닝 구분'
                ,'작성자'
                ]]
    
    # fileUrl table 가져오기
    df_file = pd.DataFrame(ExtractAttachments(len(df_index)
                                                , df_index
                                                , "secret_key"))


    df_merge_outer = pd.merge(df_index, df_file, how='outer', on= 'pageId') # fileUrl 테이블과 leftjoin
    df_merge_fillna = df_merge_outer.fillna('') # NaN 셀 빈칸으로 전환
    df_merge_sort = df_merge_fillna.sort_values('접수일자') # 클레임접수일자 기준 정렬 



    return df_merge_sort




