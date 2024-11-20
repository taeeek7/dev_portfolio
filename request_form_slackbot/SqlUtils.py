from datetime import datetime
from dotenv import load_dotenv
from SlackUtils import SlackUtils
import pymysql
import pandas as pd
import numpy as np
import os
  
# 환경변수 로드 
load_dotenv()
slack_error_token = os.getenv("SLACK_TOKEN")
slack_error_channel = "C06FQURRGCS"
slack_client = SlackUtils(slack_error_token, slack_error_channel)


# 전역변수 설정
execute_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

## 데이터베이스 출력 설정 클래스
class SqlUtils : 
        
    # 기본값 설정
    def __init__(self, *args) :
        self.host = args[0]
        self.user = args[1]
        self.password = args[2]
        self.db = args[3]
        self.selectQuery = args[4]


    # 데이터 추출 함수 정의 
    def extract_db(self) :
            
        #전역변수 선언부
        conn = None
        cur = None
        sql=""

        conn = pymysql.connect(host = self.host
                                ,user = self.user
                                ,password = self.password
                                ,db = self.db
                                ,charset="utf8"
                        )

        #실행할 sql 구문
        sql= self.selectQuery

        #커서생성
        cur = conn.cursor()

        # cursor 객체를 이용해서 수행
        cur.execute(sql)
        result = cur.fetchall()

        conn.commit()
        conn.close()

        df = pd.DataFrame(result).fillna('')
        
        return df

    # 별도 import한 dataframe을 insert 쿼리문으로 가공 처리 함수 정의
    def insert_setting_format(data) :
        rowCnt = data.shape[0]
        colCnt = data.shape[1]
        cell = ''
        for j in range (0, rowCnt) :
                cell += "("
                
                for i in range (0, colCnt) :
                        if data.iloc[j,i] == '' : 
                                cell += "null"
                        elif data.iloc[j,i] is None :
                                cell += "null"
                        elif type(data.iloc[j,i]) == np.int64 :
                                cell += data.iloc[j,i].astype(str)
                        elif type(data.iloc[j,i]) == np.float64 :
                                type_change = int(data.iloc[j,i])
                                cell += str(type_change)
                        elif type(data.iloc[j,i]) == float :
                                type_change = int(data.iloc[j,i])
                                cell += str(type_change)        
                        elif type(data.iloc[j,i]) == str :
                                cell += "'" + data.iloc[j,i] + "'"
                        elif type(data.iloc[j,i]) == pd.Timestamp :
                                cell += "'" + data.iloc[j,i].strftime("%Y-%m-%d %T") + "'"
                        else :
                                cell += "null"
                                
                        if i < colCnt - 1 : 
                                cell += ","
                        else :
                                ''
                if j < rowCnt - 1 :
                        cell += "),"
                else :
                        cell += ")"
        
        return cell
    
    # 데이터 값 insert 실행 함수
    def insert_data_value(self, table, value) : 
        #예외처리 슬랙봇 
        try : 
            # 업데이트 데이터 유무 확인 조건문
            if value == '' :
                print(execute_datetime, f"{table} insert", "insert 데이터가 존재하지 않습니다.")
            else :
                #전역변수 선언부
                conn = None
                cur = None
                sql=""

                conn = pymysql.connect(  host = self.host
                                        ,user = self.user
                                        ,password = self.password
                                        ,db = self.db
                                        ,charset="utf8"
                                )
                
                #실행할 sql 구문
                sql= self.selectQuery

                #커서생성
                cur = conn.cursor()

                # cursor 객체를 이용해서 수행
                cur.execute(sql)

                conn.commit()
                conn.close()

                print(execute_datetime, f"{table} insert", f"데이터 insert 완료")
    
        except Exception as e :
                slack_client.send_messages(text= f"*🤬 {table} insert 오류 알림*\n\n ● 오류내용 : {e}\n\n ● query : {sql}\n")
                raise e