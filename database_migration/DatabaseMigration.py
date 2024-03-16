from SlackBot import SlackBot
from datetime import datetime
import pymysql
import pandas as pd 
import numpy as np 

execute_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

## 데이터베이스 출력 설정 클래스

class SetOutputDatabase : 
        
        # 기본값 설정
        def __init__(self, *args) :
                self.host = args[0]
                self.user = args[1]
                self.password = args[2]
                self.db = args[3]
                self.selectQuery = args[4]


        # 데이터 추출 함수 정의 
        def extract_db(self) :

                import pymysql
                import pandas as pd 
                import numpy as np 

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
        

        # insert 쿼리문 가공 함수 정의
        def insert_setting(self) :
                
                data = SetOutputDatabase.extract_db(self)
                
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
                                        cell += data.loc[j,i].astype(str)
                                elif type(data.iloc[j,i]) == np.float64 :
                                        type_change = int(data.loc[j,i])
                                        cell += str(type_change)
                                elif type(data.iloc[j,i]) == float :
                                        type_change = int(data.loc[j,i])
                                        cell += str(type_change)        
                                elif type(data.iloc[j,i]) == str :
                                        cell += "'" + data.loc[j,i] + "'"
                                elif type(data.iloc[j,i]) == pd.Timestamp :
                                        cell += "'" + data.loc[j,i].strftime("%Y-%m-%d %T") + "'"
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

        

## 데이터베이스 편집 클래스

class EditDatabase :
        
        # 기본값 설정
        def __init__(self, *args) :
                self.host = args[0]
                self.user = args[1]
                self.password = args[2]
                self.db = args[3]        

        # 데이터 값 CRUD 실행 함수
        def insert_data_value(self, table, value, query) : 
                print(execute_datetime)
                print(f"파이썬실행_GCP {table} insert")

                #예외처리 슬랙봇 
                try : 

                        # 업데이트 데이터 유무 확인 조건문
                        if value == '' :
                                print("insert 데이터가 존재하지 않습니다.")
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
                                sql= query

                                #커서생성
                                cur = conn.cursor()

                                # cursor 객체를 이용해서 수행
                                cur.execute(sql)

                                conn.commit()
                                conn.close()

                                print(f"데이터 insert 완료")
                
                except Exception as e :
                        crontab_error = SlackBot("slackbotKey"
                                                ,"channel_id")
                        crontab_error.send_messages(text= f"*🤬 파이썬실행_GCP {table} insert 오류 알림*\n\n"
                                                        +f"   ● 오류내용 : {e}\n" 
                                                )
                        print(e)
        