from SlackBot import send_slackbot
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import gspread
import pandas as pd
import time
import requests
import json


### 키퍼 포인트 조정 자동화 API 연동
### 변수 항목 :
# jsonKey, 스프레드시트id, 시트명, 업로드시트명 


def fn_modify_point(jsonKey, sheetKey, wsName, uploadName) :
    
    ### 구글 문서 업로드 리스트 가져오기 ###
    
    # 인증 정보 로드
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(jsonKey, scope)
    client = gspread.authorize(creds)

    # 구글 스프레드시트 불러오기
    doc = client.open_by_key(sheetKey)
    
    # 구글 시트 변수 선언
    ws = doc.worksheet(wsName)
    upload = doc.worksheet(uploadName)
    checkCell = pd.DataFrame(ws.get('C7'))
    stateCell = pd.DataFrame(ws.get('C9'))
    df_upload = pd.DataFrame(upload.get_all_values())
    
    # 실행 변수 선언
    numRow = len(df_upload)
    responseDate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # API 호출 결과값 설정 변수
    successCnt = 0
    errorCnt = 0
    errorKeeperId = []
    errorData = []
    

    ### 실행 조건 설정 ###
    
    # 확인필요 건 및 실행 상태 확인
    if (checkCell.iloc[0,0] == '0건' and stateCell.iloc[0,0] == '대기') :
        
        # 실행 건수 확인
        if numRow > 1 :
            
            # 실행 반복문
            for i in range(1,numRow) :

                # 파라미터 설정
                memberKeeperId = df_upload.iloc[i,0]
                pointModifyCode = df_upload.iloc[i,1]
                workPoint = df_upload.iloc[i,2]
                pointModifyComment = df_upload.iloc[i,3]


                # API 엔드포인트 URL
                url = "url"

                # 요청 헤더 설정
                headers = {
                    "memberAdminId": "1" ,
                    "level": "90",
                    "Content-Type": "application/json;charset=UTF-8",
                }

                # 요청 본문 데이터 설정
                data = {
                      "memberKeeperId": memberKeeperId
                    , "pointModifyCode": pointModifyCode
                    , "workPoint": workPoint
                    , "pointModifyComment": pointModifyComment
                }

                # POST 요청 보내기
                responseAPI = requests.post(url, headers=headers, data=json.dumps(data))
                result = json.loads(responseAPI.text)

                if result['error'] == None :
                    successCnt = successCnt + 1 
                else : 
                    errorCnt = errorCnt + 1
                    errorKeeperId.append(df_upload.iloc[i,0])
                    errorData.append(result['error']['detail'])

            
            ### 결과 슬랙봇 발송 함수 
            send_slackbot("slack_key"
                            ,"slack_id"
                            ,"🪄 *키퍼 포인트 자동 조정*\n\n" 
                            + f"   ● 실행일시 : {responseDate}\n"
                            + f"   ● 실행건수 : {numRow-1} 건\n"
                            + f"   ● 결과 : 조정완료 {successCnt} 건 / 실패 {errorCnt} 건 \n"
                            + f"         ○ error_keeperId : {errorKeeperId}\n"
                            + f"         ○ error_data : {errorData}\n"
                            )
            
            # 구글 시트 업데이트
            if errorCnt >= 1 : 
                ws.update('C9','중지')
            else : 
                ws.update('C9','완료')

            # 터미널 결과창 출력
            print(responseDate)
            print("파이썬실행_키퍼포인트조정")
            print(f"{numRow-1}건이 실행되었습니다")
            print(f"결과 : 조정완료 {successCnt} 건 / 실패 {errorCnt} 건")
        
        
        else : 
            # 터미널 결과창 출력
            print(responseDate)
            print("파이썬실행_키퍼포인트조정")
            print("실행건이 없습니다.")
    
    else : 
        # 터미널 결과창 출력
        print(responseDate)
        print("파이썬실행_키퍼포인트조정")
        print(f"확인필요 {checkCell.iloc[0,0]} / 실행상태 {stateCell.iloc[0,0]}")

