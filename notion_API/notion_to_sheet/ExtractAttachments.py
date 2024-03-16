from numpyencoder import NumpyEncoder
from  datetime import datetime
import requests, json
import pandas as pd
import numpy as np 
import time
import json


## 페이지id로 첨부파일 속성의 url 추출
#변수: 추출하려는 id 개수, id가 담긴 데이터프레임명

def ExtractAttachments(numId, dataFrameId, notionApiKey) : 
        fileData = []

        #페이지id 개수만큼 반복
        for i in range (0, numId) :
            
            #페이지id 변수 지정
            pageId = dataFrameId.iloc[i,0]

            ### 노션 Page ID 및 API Token 변수 입력 ###
            token = notionApiKey
            notion_url = f"https://api.notion.com/v1/pages/{pageId}"
            headers = {
                "Authorization": "Bearer " + notionApiKey,
                "Notion-Version": "2022-02-22"
            }

            #API호출
            req = requests.get(url=notion_url, headers=headers)
            data = req.json()

            #페이지 ID별 첨부파일 개수 확인
            attachments = data.get('properties', {}).get('해당사진').get('files')
            fileType = [block for block in attachments if block['type'] == 'file']
            numFileType = len(fileType)

            #첨부파일 개수 만큼 url 추출
            if numFileType > 0 :
                for i in range (0,numFileType) :
                    try :
                        fileData.append([pageId,data['properties']['해당사진']['files'][i]['file']['url'],numFileType])
                    except :
                        ''
            else : 
                fileData.append([pageId, ''])
        
        # page_id <> url dataframe화 
        df_temp = pd.DataFrame(fileData)
        
        # column_name 지정
        df_temp['pageId'] = df_temp[0]
        df_temp['url'] = df_temp[1]
 
        df_rename = df_temp[['pageId', 'url']]

        # 동일한 page_id끼리 url 묶어주기
        df_file = df_rename.groupby('pageId')['url'].apply(lambda x:','.join(x)).reset_index()
        
        # df_file dataFrame 반환
        return df_file


