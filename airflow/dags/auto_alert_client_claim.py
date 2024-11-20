from __future__ import annotations
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from dateutil.relativedelta import relativedelta
from pandasql import sqldf
from datetime import datetime
from utils.slack import SlackUtils
from utils.sql import SqlUtils
from templates.client_claim import GetData, MessageTemplate
import pendulum
import pandas as pd
import os

# 실패 알림
def notify_failure(context) :
    SlackUtils.notify_failure(context)

# 고객클레임 접수 건 객실 key_value 확인
def get_client_claim_list_key() :
    source_data = SqlUtils.get_source_data(
        conn_id= "cleanops",
        sql = """
        select 
            concat(cl_cd, "_", branch_id, "_", room_id)
        from client_claim_list
        ;
        """
    )
    df = pd.DataFrame(source_data)
    return df

ROOM_KEY = get_client_claim_list_key()[0].tolist()

# 고객클레임 객실 오더 건 찾기
def search_claim_order_no() :
    claim_raw_data = SqlUtils.get_source_data(
        conn_id= "cleanops",
        sql= """
        select root_trigger_id, reception_username, reception_date, cl_cd, branch_id, room_id, room_no, room_validation, reception_contents, check_in_claim
        from client_claim_list
        where order_no is null
        ;
        """
    )
    claim_list = pd.DataFrame(claim_raw_data)

    if len(claim_list) == 0 :
        return claim_list
    else : 
        claim_list_rename = {
            0: 'root_trigger_id'
            ,1: 'reception_username'
            ,2: 'reception_date'
            ,3: 'cl_cd'
            ,4: 'branch_id'
            ,5: 'room_id'
            ,6: 'room_no'
            ,7: 'room_validation'
            ,8: 'reception_contents'
            ,9: 'check_in_claim'
        }
        claim_list.rename(columns= claim_list_rename, inplace= True)

        keeper_raw_data = SqlUtils.get_source_data(
            conn_id= "prod-keeper",
            sql= f"""
            select 
                ko.order_no
                ,t.ticket_id
                ,t.cl_cd
                ,t.branch_id
                ,t.room_id
                ,DATE_FORMAT(ko.end_at, "%Y-%m-%d 00:00:00") as end_at  
                ,t.ticket_code 
                ,rp.depth2_cost 
                ,rp.depth3_cost 
                ,op.member_keeper_id
                ,case when rt.bedroom = 'one' then 10 
                    when rt.bedroom = 'one_half' then 15 
                    when rt.bedroom = 'two' then 20 
                    when rt.bedroom = 'three' then 30  
                    end as point
                ,oc.grade_calculate
                ,oc.score
            from keeper_order as ko 
            inner join ticket as t 
                on ko.ticket_id = t.ticket_id
            inner join client as c 
                on t.cl_cd = c.cl_cd
            inner join branch as b 
                on t.cl_cd = b.cl_cd
                and t.branch_id = b.branch_id
            inner join ticket_type as tt
                on t.ticket_code = tt.code
            inner join room_price as rp
                on t.cl_cd = rp.cl_cd
                and t.branch_id = rp.branch_id
                and t.room_id = rp.room_id
                and t.ticket_code = rp.code
            inner join order_party as op
                on ko.keeper_order_id = op.keeper_order_id
            inner join roomtype as rt 
                on t.cl_cd = rt.cl_cd
                and t.branch_id = rt.branch_id
                and t.roomtype_id = rt.roomtype_id
            inner join order_complete as oc 
                on ko.keeper_order_id = oc.keeper_order_id
                and ko.order_no = oc.order_no
            where
                ko.order_status = 'COMPLETE'
                and ko.end_at BETWEEN DATE_FORMAT(DATE_SUB(CURRENT_DATE(), INTERVAL 30 day), "%Y-%m-%d 00:00:00") and DATE_FORMAT(DATE_SUB(CURRENT_DATE(), INTERVAL 1 day), "%Y-%m-%d 23:59:59")
                and t.ticket_code in ('R', 'ED', 'EC', 'EMC', 'EMD')
                and concat(t.cl_cd,"_",t.branch_id,"_",t.room_id) in ({str(ROOM_KEY)[1:-1]})
                and op.is_main = 1
            ;
            """
        )
        keeper_order = pd.DataFrame(keeper_raw_data)

        keeper_order_rename = {
            0: 'order_no'
            ,1: 'ticket_id'
            ,2: 'cl_cd'
            ,3: 'branch_id'
            ,4: 'room_id'
            ,5: 'end_at'
            ,6: 'ticket_code'
            ,7: 'depth2_cost'
            ,8: 'depth3_cost'
            ,9: 'member_keeper_id'
            ,10: 'point'
            ,11: 'grade_calculate'
            ,12: 'inspect_score'
        }
        keeper_order.rename(columns= keeper_order_rename, inplace= True)

        # claim_list <> keeper_order join pandasql
        join_query = """
        with temp as (
            select 
                cl.root_trigger_id
                ,cl.reception_username
                ,cl.reception_date
                ,cl.cl_cd
                ,cl.branch_id
                ,cl.room_id
                ,cl.room_no
                ,cl.room_validation
                ,cl.reception_contents
                ,cl.check_in_claim
                ,ko.order_no
                ,ko.ticket_id
                ,ko.end_at
                ,ko.ticket_code
                ,ko.depth2_cost
                ,ko.depth3_cost
                ,ko.member_keeper_id
                ,ko.point
                ,ko.grade_calculate
                ,ko.inspect_score
                ,ROW_NUMBER() OVER (PARTITION BY cl.cl_cd, cl.branch_id, cl.room_id order by ko.end_at desc) as rn
            from claim_list as cl 
            left join keeper_order as ko 
                on cl.cl_cd = ko.cl_cd
                and cl.branch_id = ko.branch_id
                and cl.room_id = ko.room_id    
            where CAST( JULIANDAY(cl.check_in_claim) - JULIANDAY(ko.end_at) AS INTEGER) >= 0
        )
        select 
            root_trigger_id
            ,reception_username
            ,reception_date
            ,cl_cd
            ,branch_id
            ,room_id
            ,room_no
            ,room_validation
            ,reception_contents
            ,check_in_claim
            ,order_no
            ,ticket_id
            ,end_at
            ,ticket_code
            ,depth2_cost
            ,depth3_cost
            ,member_keeper_id
            ,point
            ,grade_calculate
            ,inspect_score
        from temp
        where rn = 1
        ;
        """
            
        return sqldf(join_query)

# 매칭된 고객클레임 오더건 데이터 insert
def insert_order_list_handler() : 
    response_date = (datetime.now() + relativedelta(hours=9)).strftime("%Y-%m-%d %H:%M:%S")
    df = search_claim_order_no()
    if len(df) == 0 :
        return f"{response_date}, 접수건이 없습니다."
    else :
        for i in range(0, len(df)) :

            # 변수설정
            root_trigger_id = df.iloc[i,0]
            order_no = df.iloc[i,10]
            ticket_id = df.iloc[i,11]
            end_at = df.iloc[i,12]
            ticket_code = df.iloc[i,13]
            depth2_cost = df.iloc[i,14]
            depth3_cost = df.iloc[i,15]
            member_keeper_id = df.iloc[i,16]
            point = df.iloc[i,17]
            grade_calculate = df.iloc[i,18]
            inspect_score = df.iloc[i,19]

            SqlUtils.crud_data(
                conn_id= 'cleanops',
                sql= f"""
                    update client_claim_list 
                    set order_no = '{order_no}',
                        ticket_id = {ticket_id},
                        end_at = '{end_at}',
                        ticket_code = '{ticket_code}',
                        depth2_cost = {depth2_cost},
                        depth3_cost = {depth3_cost},
                        member_keeper_id = {member_keeper_id},
                        point = {point},
                        grade_calculate = {grade_calculate},
                        inspect_score = {inspect_score}
                    where root_trigger_id = '{root_trigger_id}'
                    ;
                    """
            )
        return

# 차감 데이터 필터 함수
def filter_penalty_list(company_name) :
    df = GetData.get_claim_person(company_name= company_name)
    df_filter = df[['root_trigger_id'
                    ,'member_keeper_id'
                    ,'order_no'
                ]]
    return df_filter

# 차감 데이터 insert
def insert_claim_point_log(df) :
    SqlUtils.crud_data(
        conn_id= "cleanops",
        sql= f"""
        REPLACE INTO claim_set_point_log(
                root_trigger_id
                ,member_keeper_id
                ,order_no
        ) VALUES {SqlUtils.insert_setting_format(data= df)}
        """
    )
    return 

# 리마인더 알림 raw data
def reminder_data() :
    reminder_data = SqlUtils.get_source_data(
        conn_id= "cleanops",
        sql= """
        select 
            cspl.insert_at
            ,cspl.member_keeper_id
            ,cspl.message_ts
            ,cspl.channel_id
            ,mk.name as keeper
            ,concat("http://11clock.slack.com/archives/",cspl.channel_id,"/p",replace(cspl.message_ts, ".", "")) as message_link
        from claim_set_point_log as cspl
        inner join member_keeper as mk 
            on cspl.member_keeper_id = mk.member_keeper_id
        where 
            cspl.modify_status = 'init' 
            and cspl.insert_at <= DATE_SUB(CURRENT_DATE(), interval 3 day)
        ;
        """
    )
    
    return pd.DataFrame(reminder_data)

# 고객클레임 건 슬랙 알림
def send_message_handler() :
    #환경변수 불러오기
    slack_token = os.getenv("AIRFLOW__SLACK__CLAIM")
    df_ch_id = GetData.get_slack_channel()
    
    for i in range(0, len(df_ch_id)) :
        company_name = df_ch_id.iloc[i,1]
        channel = df_ch_id.iloc[i,3]
        represent = df_ch_id.iloc[i,5]

        df = GetData.get_claim_person(company_name= company_name)

        if len(df) == 0 :
            print("대상자 없음")
        else :
            slack_client = SlackUtils(slack_token, channel)
            
            # 슬랙봇 발송 - 공통 섹션
            slack_client.send_block_kit(
                text= "대상자 리스트",
                blocks= MessageTemplate.common_section(represent= represent)
            )
            # 슬랙봇 발송 - 대상자 리스트
            slack_client.send_block_kit(
                text= "대상자 리스트",
                blocks= MessageTemplate.penalty_list_section(company_name= company_name)
            )
            
            # 포인트 차감 로그 적재
            insert_claim_point_log(filter_penalty_list(company_name= company_name))
            print(f'{company_name} 포인트 차감 로그 적재 완료')
    
    return 

# 리마인더 슬랙 알림 발송
def send_reminder_handler() :
    #환경변수 불러오기
    slack_token = os.getenv("AIRFLOW__SLACK__CLAIM")
    df = reminder_data()
    if len(df) == 0 :
        print("리마인더 대상 없음")
    else :
        for i in range(0, len(df)) :
            message_ts = df.iloc[i,2]
            channel_id = df.iloc[i,3]
            keeper = df.iloc[i,4]
            message_link = df.iloc[i,5]
            
            slack_client = SlackUtils(slack_token, channel_id)

            slack_client.send_messages(text= f"*🔔 고객클레임봇 리마인더* \n\n {keeper} 키퍼님의 포인트 감점 내용을 확인해주세요.\n 링크: {message_link}")  
            
    return 

with DAG(
    dag_id="auto_alert_client_claim", # dag_id - 보통 파일명과 동일하게 
    schedule="0 10 * * *", # cron 스케줄
    start_date=pendulum.datetime(2024, 9, 2, 10, 0, tz="Asia/Seoul"), # 이전 스케줄 실행 date와 일치시키자
    catchup=False, # 과거 데이터 소급적용
    tags=["automation", "alert", "client_claim"], # 태그값
    default_args= {
        'on_failure_callback' : notify_failure
    }
    
) as dag:
    task_t1_search_order = PythonOperator(
        task_id='task_t1_search_order',
        python_callable=insert_order_list_handler
    )
    task_t2_send_message = PythonOperator(
        task_id='task_t2_send_message',
        python_callable=send_message_handler
    )
    task_t3_send_reminder = PythonOperator(
        task_id='task_t3_send_reminder',
        python_callable=send_reminder_handler
    )


    task_t1_search_order >> task_t2_send_message >> task_t3_send_reminder
    # task_t3_send_reminder