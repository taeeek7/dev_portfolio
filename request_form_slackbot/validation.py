from dotenv import load_dotenv
from SqlUtils import SqlUtils
import os
import inspect
  
# 환경변수 import 
load_dotenv()
host = os.getenv("N_HOST")
user = os.getenv("N_USER")
password = os.getenv("N_PASSWORD")
db = os.getenv("N_DB")

class Validation :
    def __init__(self) :
        pass
    # 객실번호 room_id 찾기
    def find_room_id(key_value, dong, room_ho) :
        df = SqlUtils(host, user, password, db, 
                f"""
                with temp_room as (
                    select 
                        CONCAT(cl_cd,'_',branch_id) as key_value, 
                        room_no, 
                        room_id,
                        case when dong is null then '없음' 
                            when dong = '' then '없음' 
                            else dong end as dong,
                        ho
                    from room 
                    where 
                        status not in ('deactive') 
                        and is_delete = 0
                    )
                    select *
                    from temp_room
                    where 
                        key_value = '{key_value}'
                        and dong = '{dong}'
                        and ho = {room_ho}
                ;
                """).extract_db()
        
        if len(df) == 1 :
            return df.iloc[0,2]
        else : 
            return 0

    # 객실번호 room_no 찾기
    def find_room_no(key_value, dong, room_ho) :
        df = SqlUtils(host, user, password, db, 
                f"""
                with temp_room as (
                    select 
                        CONCAT(cl_cd,'_',branch_id) as key_value, 
                        room_no, 
                        room_id,
                        case when dong is null then '없음' 
                            when dong = '' then '없음' 
                            else dong end as dong,
                        ho
                    from room 
                    where 
                        status not in ('deactive') 
                        and is_delete = 0
                    )
                    select *
                    from temp_room
                    where 
                        key_value = '{key_value}'
                        and dong = '{dong}'
                        and ho = {room_ho}
                ;
                """).extract_db()
        
        if len(df) == 1 :
            return df.iloc[0,1]
        else : 
            return 0
    
    # 카테고리 메서드 찾기
    def find_category_method(category_name) :
        df = SqlUtils(host, user, password, db, 
                f"""
                select
                    modal_format
                    ,call_index
                from client_request_category_item
                where 
                    status in ('active') 
                    and requester = 'handys'
                    and category = '{category_name}'
                ;
                """
                ).extract_db()
        
        return df.iloc[0,0],df.iloc[0,1]

    # 지점별 동 리스트 추출
    def find_room_building(key_value) :
        df = SqlUtils(host, user, password, db, 
                f"""
                select
                    cl_cd
                    ,branch_id
                    ,case 
                        when dong is null then '없음'
                        WHEN dong = '' THEN '없음'
                        else dong end as building
                from room 
                where 
                    status not in ('deactive') 
                    and is_delete = 0
                    and concat(cl_cd,"_",branch_id) = '{key_value}'
                group by 1,2,3 
                ;
                """).extract_db()

        return df 
    
    # 접수자 DB 추출
    def find_receipt_user(root_trigger_id) :
        df = SqlUtils(host, user, password, db, 
                f"SELECT reception_user_id FROM client_request_list where root_trigger_id = '{root_trigger_id}'").extract_db()
        return df.iloc[0,0]

    # class 안에 method 찾기
    def get_class_methods(cls):
        methods = inspect.getmembers(cls, predicate=inspect.isfunction)
        method_names = [name for name, _ in methods]
        return method_names 

    # class 안 method 호출 
    def call_method_by_index(instance, method_names, index, *args, **kwargs):
        if index < 0 or index >= len(method_names):
            raise IndexError("Invalid method index")
        
        method_name = method_names[index]
        method = getattr(instance, method_name)
        return method(*args, **kwargs)