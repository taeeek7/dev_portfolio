from airflow.providers.mysql.hooks.mysql import MySqlHook
from utils.gsheet import GsheetUtils
import json
import numpy as np
import pandas as pd

class SqlUtils :
    def extract_data(conn_id, sql) :
        source_hook = MySqlHook(mysql_conn_id= conn_id)
        source_data = source_hook.get_records(sql= sql)
        
        return json.loads(json.dumps(source_data, default= GsheetUtils.decimal_default))
    
    def get_source_data(conn_id, sql) :
        source_hook = MySqlHook(mysql_conn_id= conn_id)
        source_data = source_hook.get_records(sql= sql)
        
        return source_data
    
    def crud_data(conn_id, sql) :
        target_hook = MySqlHook(mysql_conn_id= conn_id)
        target_conn = target_hook.get_conn()
        cursor = target_conn.cursor()

        cursor.execute(sql)

        target_conn.commit()
        cursor.close()
        target_conn.close()
        
        print("Execute SQL Success")
        return
    
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