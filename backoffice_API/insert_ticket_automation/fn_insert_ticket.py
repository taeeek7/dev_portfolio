from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from datetime import datetime
import gspread
import pandas as pd
import time
import requests
import json


def v3_insert_ticket(sheet_key, json_key, ws_name, upload_name, channel_id) :

    # 인증 정보 로드
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_key, scope)
    client = gspread.authorize(creds)

    # 구글 스프레드시트 불러오기
    doc = client.open_by_key(sheet_key)
    
    #슬랙 메시지 발송 변수 설정
    slack_token = "token"
    client = WebClient(token=slack_token)
    
    #지점별 시트 변수 지정
    ws = doc.worksheet(ws_name)
    upload = doc.worksheet(upload_name)
    df = pd.DataFrame(upload.get_all_values())
    df_br = pd.DataFrame(ws.get('B1'))
    df_s = pd.DataFrame(ws.get('B4'))

    # 실행 변수 지정
    branch = df_br.loc[0,0]    # 지점명
    last_row = len(df) - 1    # 발행건수확인
    condition = df_s.loc[0,0] # 조건 확인
    response_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # API 호출 결과값 설정 변수
    success_cnt = 0
    error_cnt =  0
    complete_y = 0 
    error_roomNo = []
    error_data = []
    complete_room = []

    # 키퍼 배정 건수 설정 변수
    assign_cnt = 0

    ### 실행조건설정 ### 
    if condition == '발행대기' : 

        # 터미널 결과창 입력
        print(response_date)

        # 티켓생성 반복문
        for i in range(1,last_row+1) : 
            
            # 사용할 변수 값을 설정
            clCd = df.loc[i,0]
            branchId = df.loc[i,1]
            roomId = df.loc[i,2]
            ticketCode = df.loc[i,3]
            emergencyCode = df.loc[i,4]
            emergencyComment = df.loc[i,5]
            searchDate = df.loc[i,6]
            roomNo = df.loc[i,7]
            memberKeeperId = df.loc[i,8]
            complete_yn = df.loc[i,9]
            
            
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
                "clCd" : clCd,
                "branchId" : branchId,
                "roomId" : roomId,
                "ticketCode" : ticketCode,
                "emergencyCode" : emergencyCode,
                "emergencyComment" : emergencyComment,
                "searchDate" : searchDate,
                "memberKeeperId": memberKeeperId,
                }

            # POST 요청 보내기
            if complete_yn == '0' : 
                response_keeper = requests.post(url, headers=headers, data=json.dumps(data))
                result = json.loads(response_keeper.text)

                # API 에러 여부 확인 (단건 조회)
                if result['error'] == None :
                    success_cnt = success_cnt + 1
                    if df.loc[i,8] == '' :
                        assign_cnt = assign_cnt + 0 
                    else : assign_cnt = assign_cnt + 1 
                else : 
                    error_roomNo.append(roomNo)
                    error_data.append(result['error']['detail'])
                    error_cnt = error_cnt + 1
            else : 
                complete_y = complete_y + 1
                complete_room.append(roomNo)

        # API 호출 결과 슬랙메시지 발송
        try:
            response_slack = client.chat_postMessage(
                channel= channel_id ,      # 채널 id를 입력합니다. C05PKAP3PK6 
                text=     f"🔖 *{branch} 긴급티켓 자동발행*\n\n" 
                        + f"   ● 실행일시 : {response_date}\n"
                        + f"   ● 실행건수 : {last_row} 건\n"
                        + f"   ● 결과 : 발행완료 총 {success_cnt} 건 (배정 {assign_cnt} 건)  //  발행실패 {error_cnt} 건  //  수행이력 존재 {complete_y} 건\n"
                        + f"         ○ error_roomNo : {error_roomNo}\n"
                        + f"         ○ error_data : {error_data}\n"
                        + f"         ○ complete_yes_roomNo : {complete_room}"
                        )
        except SlackApiError as e:
            assert e.response["error"]
        
        # 구글 시트 업데이트
        if error_cnt >= 1 : 
            ws.update('B4','발행중지')
        else : ws.update('B4','발행완료')

        #터미널 결과창 입력
        print(f"파이썬실행_긴급티켓발행_{branch}")
        print(  
              f"성공 {success_cnt} 건 / 배정 {assign_cnt} 건\n"
            + f"실패 {error_cnt} 건   / 수행이력존재 {complete_y}건\n"
            + f"{error_roomNo}\n"
            + f"{error_data}\n"
            + f"{complete_room}")
        

    else : 
        print(response_date)
        print(f"파이썬실행_긴급티켓발행_{branch}")
        print("발행대기 중이 아닙니다.")


