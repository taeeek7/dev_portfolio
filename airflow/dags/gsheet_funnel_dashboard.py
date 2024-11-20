from __future__ import annotations
from airflow import DAG
from airflow.providers.google.suite.operators.sheets import GSheetsHook
from oauth2client.service_account import ServiceAccountCredentials
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator
from dateutil.relativedelta import relativedelta
from utils.slack import SlackUtils
import pendulum
import gspread
import os
import json
import decimal

# Google Sheets 스프레드시트 ID 및 시트 이름 설정
SPREADSHEET_ID = '1GLWPyJP9jLSwARAccpOj9nxper29yg0dvLWUtq66f4A'
SHEET_KEEPER = '[raw]키퍼정보'
SHEET_JOIN = '[raw]신규가입지점'
SHEET_REVENUE = '[raw]매출&수행건'
SHEET_KEY = '/opt/airflow/services/keeper-data-4c16ed1166b5.json'

def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError

def notify_failure(context):
    #환경변수 불러오기
    scheduler_token = os.getenv("AIRFLOW__SLACK__SCHEDULER")
    # 객체 생성
    slack_alert = SlackUtils(scheduler_token, "C07H689UA2K")

    # context에서 태스크 및 DAG 정보 가져오기
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    execution_date = (context.get('execution_date') + relativedelta(hours=9)).strftime('%Y-%m-%d %H:%M:%S')
    log_url = task_instance.log_url
    domain_url = log_url.replace("http://localhost:8080", "airflow.11h.kr")

    # Slack 메시지 전송
    slack_alert.fail_msg(
        text= f"""
        💨 *Airflow Scheduler Alert*\n
        ● result: fail\n
        ● dag_id: {dag_id}\n
        ● task_id: {task_id}\n
        ● execution_date: {execution_date}\n
        ● log_url: {domain_url}
        """
    )

# 키퍼정보
def extract_keeper_info() :
    source_hook = MySqlHook(mysql_conn_id= "prod-keeper")
    source_data = source_hook.get_records(
        sql= """
        WITH keeper_table AS (
            SELECT 
            mk.member_keeper_id AS keeper_id, 
            mk.name AS keeper_name,
            g.grade_name AS grade,
            REPLACE(concat(mk.name,"(",g.grade_name,")")," ","") AS name_grade, 	   
            IFNULL(fn_get_code_name('KEEPER_STATUS', mk.state_code), '강제삭제') as state_code,
            IFNULL(fn_get_code_name('FUNNEL', mk.funnel), '알수없음') as funnel,
            CASE WHEN mid(CAST(AES_DECRYPT(UNHEX(mk.personal_id), 'personal_id') AS CHAR),7,1) IN (1,3)  
            THEN '남성'
            WHEN mid(CAST(AES_DECRYPT(UNHEX(mk.personal_id), 'personal_id') AS CHAR),7,1) IN (2,4)
            THEN '여성'
            ELSE '알수없음' END AS gender,
            CASE WHEN mid(CAST(AES_DECRYPT(UNHEX(mk.personal_id), 'personal_id') AS CHAR),7,1) IN (1,2)  
            THEN DATE_FORMAT(CURRENT_DATE(), '%Y') - (LEFT(CAST(AES_DECRYPT(UNHEX(mk.personal_id), 'personal_id') AS CHAR),2) + 1900)
            WHEN mid(CAST(AES_DECRYPT(UNHEX(mk.personal_id), 'personal_id') AS CHAR),7,1) IN (3,4)
            THEN DATE_FORMAT(CURRENT_DATE(), '%Y') - (LEFT(CAST(AES_DECRYPT(UNHEX(mk.personal_id), 'personal_id') AS CHAR),2) + 2000)
            ELSE 0 END AS age,
            DATE_FORMAT(mk.create_at, '%Y-%m-%d') AS join_at
            FROM member_keeper mk
            LEFT JOIN grade g
            ON mk.grade_id = g.grade_id
            LEFT JOIN branch b 
            ON mk.cl_cd = b.cl_cd
            AND mk.branch_id = b.branch_id
            WHERE mk.LEVEL = 30 
            and mk.name not like '%키퍼%'
            and mk.name not like '%keeper%'
            and mk.name not like '%룸세팅패스%'
            ),
            cleaning_table AS (
            SELECT  op.member_keeper_id AS keeper_id,
            count(keeper_order_id) AS cnt
            FROM order_party op
            GROUP BY 1
            )
            SELECT  kt.keeper_id,
            kt.keeper_name,
            kt.grade,
            kt.name_grade,
            kt.state_code,
            kt.funnel,
            CASE WHEN kt.age >= 100 
            THEN '알수없음'
            ELSE kt.gender
            END AS gender,
            CASE WHEN kt.age = 0 
            THEN '알수없음'
            WHEN kt.age BETWEEN 20 AND 29 
            THEN '20대'
            WHEN kt.age BETWEEN 30 AND 39 
            THEN '30대'
            WHEN kt.age BETWEEN 40 AND 49 
            THEN '40대'
            WHEN kt.age BETWEEN 50 AND 59 
            THEN '50대'
            WHEN kt.age BETWEEN 60 AND 69 
            THEN '60대'
            WHEN kt.age >= 100 
            THEN '알수없음'
            ELSE '그외'		 	
            END AS age_range,
            CASE WHEN ct.cnt > 0
            THEN '수행완료'
            ELSE '수행건없음'
            END AS cleaning_yn,
            kt.join_at,
            DATE_FORMAT(kt.join_at, '%Y') AS year,
            DATE_FORMAT(kt.join_at, '%m') AS month,
            WEEK(kt.join_at,3) AS week 
            FROM keeper_table kt
            LEFT JOIN cleaning_table ct
            ON kt.keeper_id = ct.keeper_id
            ORDER BY keeper_id ASC
            ;   
        """
    )
    return json.loads(json.dumps(source_data, default=decimal_default))

# 신규가입지점정보
def extract_new_join() :
    source_hook = MySqlHook(mysql_conn_id= "prod-keeper")
    source_data = source_hook.get_records(
        sql= """
        SELECT 
            mk.member_keeper_id AS keeper_id, 
            mk.name AS keeper_name,
            IFNULL(fn_get_code_name('FUNNEL', mk.funnel), '알수없음') as funnel,
            c.cl_cm AS client,
            b.name AS branch,
            b.region,
            DATE_FORMAT(mk.create_at, '%Y-%m-%d') AS join_at,
            DATE_FORMAT(mk.create_at, '%Y') AS year,
            DATE_FORMAT(mk.create_at, '%m') AS month,
            WEEK(mk.create_at,3) AS week 
            FROM (SELECT *
            FROM member_keeper
            WHERE LEVEL = 30 
            AND name not like '%키퍼%'
            AND name not like '%keeper%'
            AND name not like '%룸세팅패스%'
            ) AS mk
            LEFT JOIN client c
            ON mk.cl_cd = c.cl_cd
            LEFT JOIN branch b 
            ON mk.cl_cd = b.cl_cd
            AND mk.branch_id = b.branch_id
            WHERE DATE_FORMAT(mk.create_at, '%Y-%m-%d') = DATE_FORMAT(DATE_SUB(current_date(), INTERVAL 1 day), '%Y-%m-%d') 
            ;
            """
    )
    return json.loads(json.dumps(source_data, default=decimal_default))

# 키퍼매출정보
def extract_revenue() :
    source_hook = MySqlHook(mysql_conn_id= "prod-keeper")
    source_data = source_hook.get_records(
        sql= """
        WITH funnel_table AS (
            SELECT 
                        op.keeper_order_id,
                        DATE_FORMAT(ko.end_at, '%Y') AS year,
                        DATE_FORMAT(ko.end_at, '%Y-%m') AS month, 
                        DATE_FORMAT(ko.end_at, '%Y-%m-%d') AS day,
                        IFNULL(fn_get_code_name('FUNNEL', mk.funnel), '알수없음') as funnel,
                        rp.depth2_cost
            FROM (  SELECT * 
                        FROM keeper_order 
                        WHERE cancel_id IS NULL
                        AND DATE_FORMAT(end_at,'%Y-%m-%d') = DATE_FORMAT(DATE_SUB(CURRENT_DATE(), INTERVAL 1 day),'%Y-%m-%d')  
                        ) AS ko 
            LEFT JOIN ( SELECT keeper_order_id, member_keeper_id
                              FROM order_party 
                              ) AS op 
                        ON ko.keeper_order_id = op.keeper_order_id
            LEFT JOIN ( SELECT *
                              FROM ticket
                              WHERE ticket_status = 'COMPLETE'
                              ) AS t 
                        ON ko.ticket_id = t.ticket_id
            LEFT JOIN member_keeper as mk 
                  on op.member_keeper_id = mk.member_keeper_id
            LEFT JOIN (SELECT room_id, cl_cd, branch_id, code, depth2_cost, depth3_cost
                              FROM room_price 
                              ) AS rp 
                        ON t.cl_cd = rp.cl_cd
                        AND t.branch_id = rp.branch_id
                        AND t.room_id = rp.room_id
                        AND t.ticket_code = rp.code	
            ),
            order_table AS (
            SELECT 
                  op.keeper_order_id, 
            count(*) as keeper_cnt
            FROM (  SELECT keeper_order_id, member_keeper_id
                        FROM order_party
                        ) AS op
            GROUP BY 1
            )
            SELECT 
                        ft.year,
                        ft.month,
                        ft.day,
                        ft.funnel,
                        CONVERT(ifnull(sum(ft.depth2_cost/ot.keeper_cnt),0),INT) as total_rev,
                        CONVERT(ifnull(count(ot.keeper_order_id),0),INT) as cleaning_cnt
            FROM funnel_table ft 
            INNER JOIN order_table ot
                        ON ft.keeper_order_id = ot.keeper_order_id
            GROUP BY 1,2,3,4
            ;
            """
    )
    return json.loads(json.dumps(source_data, default=decimal_default))

# 키퍼정보 업로드
def upload_keeper_info_handler() :
    # 인증 정보 로드
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(SHEET_KEY, scope)
    client = gspread.authorize(creds)
    
    # 구글 스프레드시트 불러오기
    doc = client.open_by_key(SPREADSHEET_ID)
    ws = doc.worksheet(SHEET_KEEPER)
    row_length = len(ws.get_all_values())

    # 기존 정보 삭제
    if row_length >= 2 : 
        ws.delete_rows(2,row_length)
        ws.append_row([""])
    else : 
        ws.append_row([""])

    #객체 생성
    hook = GSheetsHook(gcp_conn_id="google-api")
    
    # Google Sheets에 데이터 업데이트
    hook.update_values(
        spreadsheet_id=SPREADSHEET_ID,
        range_=f'{SHEET_KEEPER}!A2',  # 데이터가 삽입될 셀 범위 지정
        values=extract_keeper_info(),  # 데이터 전달
        value_input_option='RAW'  # 데이터 입력 방식 설정
    )

#신규가입지점정보 업로드
def upload_new_join_handler() :
    #객체 생성
    hook = GSheetsHook(gcp_conn_id="google-api")
    
    # Google Sheets에 데이터 업데이트
    hook.append_values(
        spreadsheet_id=SPREADSHEET_ID,
        range_=SHEET_JOIN,  # 데이터가 삽입될 셀 범위 지정
        values=extract_new_join(),  # 데이터 전달
        value_input_option='RAW'  # 데이터 입력 방식 설정
    )

# 키퍼매출정보 업로드
def upload_revenue_handler() :
    #객체 생성
    hook = GSheetsHook(gcp_conn_id="google-api")
    
    # Google Sheets에 데이터 업데이트
    hook.append_values(
        spreadsheet_id=SPREADSHEET_ID,
        range_=SHEET_REVENUE,  # 데이터가 삽입될 셀 범위 지정
        values=extract_revenue(),  # 데이터 전달
        value_input_option='RAW'  # 데이터 입력 방식 설정
    )


with DAG(
    dag_id="gsheet_funnel_dashboard", # dag_id - 보통 파일명과 동일하게 
    schedule="30 6 * * *", # cron 스케줄
    start_date=pendulum.datetime(2024, 8, 10, tz="Asia/Seoul"), # 시작일자
    catchup=False, # 과거 데이터 소급적용
    tags=["gsheet", "daily"], # 태그값
    default_args= {
        'on_failure_callback' : notify_failure
    }
    
) as dag:
    # PythonOperator로 샘플 데이터를 생성하고 Google Sheets에 업로드
    task_keeper_info = PythonOperator(
        task_id='task_keeper_info',
        python_callable=upload_keeper_info_handler
    )
    task_new_join = PythonOperator(
        task_id='task_new_join',
        python_callable=upload_new_join_handler
    )
    task_revenue = PythonOperator(
        task_id='task_revenue',
        python_callable=upload_revenue_handler
    )

    # DAG 순서 정의
    task_keeper_info >> task_new_join >> task_revenue




