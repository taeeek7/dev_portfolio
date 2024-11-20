from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk import WebClient
from datetime import datetime, timezone, timedelta
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
from template import CategoryFormat, ModalFormat
from message import MessageTemplate
from validation import Validation
from SlackUtils import SlackUtils
from SqlUtils import SqlUtils
from AwsUtils import AwsUtils
import os, re, json
import requests

# 환경변수 로드 
load_dotenv()
slack_bolt_bot_token = os.getenv("SLACK_REQUEST_BOT_TOKEN")
slack_bolt_app_token = os.getenv("SLACK_REQUEST_APP_TOKEN")
slack_bolt_secret = os.getenv("SLACK_REQUEST_SECRET")
slack_error_token = os.getenv("SLACK_TOKEN")

host = os.getenv("N_HOST") 
user = os.getenv("N_USER")
password = os.getenv("N_PASSWORD")
db = os.getenv("N_DB")

# 앱 호출
app = App(token= slack_bolt_bot_token, signing_secret= slack_bolt_secret)

# 슬랙 관련 객체생성
slack_error_channel = "C06FQURRGCS"
slack_error_client = SlackUtils(slack_error_token, slack_error_channel)

# 봇 인사
@app.message("누구냐 넌")
def message_hello(ack, message, say) :
    ack()
    if message["text"] == "누구냐 넌" :
        say("?! 내가 바로 ~ 탕탕탕ㄴ탙ㅇ후루후루훌루 🍅🍆🍑🍒🍓🍇")

# 기타 메시지 무시
@app.event("message")
def handle_message_events():
    pass

### 담당자 선택 액션 무시
@app.action("users_action")
def handle_users_action(ack) :
    ack()
    pass

# 앱 멘션 이벤트 - 업무요청작성 양식 버튼 발송
@app.event("app_mention")
def handle_message_events(ack, event):
    ack()
    try : 
        # 슬랙 메시지 발송
        WebClient(token= slack_bolt_bot_token
        ).chat_postEphemeral(
            channel= event['channel']
            ,user= event['user']
            ,text= f"업무요청양식을 열어주세요"
            ,blocks= MessageTemplate.welcome_message()
        )
    except Exception as e :
        WebClient(token= slack_bolt_bot_token).chat_postMessage(channel= 'C06FQURRGCS', text= f"열한시X핸디즈 업무요청접수오류 - 앱 멘션, {e}, <@austin>")

# process 1: 요청접수창 오픈
@app.action("open_title_modal")
def handle_title_modal(ack, body, client) :
    ack()
    try :
        root_trigger_id = body["trigger_id"]
        channel_id = body['container']['channel_id']
        # 모달 트리거
        client.views_open(
            trigger_id= root_trigger_id
            ,view= ModalFormat(root_trigger_id, channel_id,'','').title_format()
        )
    except Exception as e :
        WebClient(token= slack_bolt_bot_token
        ).chat_postEphemeral(
            channel= channel_id, 
            user= body['user']['id'],
            text= "접수창이 만료되었습니다. 다시 앱을 멘션하여 작성하기 버튼을 눌러주세요."
        )

### process 2: 요청사항 상세 내용 작성 모달 - 카테고리별 오픈 모달 자동 변경
@app.view("modal_category_format")
def handle_form_modal(ack, body, client):
    ack()
    try : 
        root_trigger_id = body['trigger_id']
        channel_id = body['view']['private_metadata']
        branch_name = body['view']['state']['values']['select_branch']['branch_select_action']['selected_option']['text']['text']
        key_value = body['view']['state']['values']['select_branch']['branch_select_action']['selected_option']['value']
        category_name = body['view']['state']['values']['select_category']['category_select_action']['selected_option']['text']['text']
        category_value = Validation.find_category_method(category_name)[0]
        call_index= Validation.find_category_method(category_name)[1]
        call_index_to_int = int(call_index)
        
        # 클래스의 메서드 이름 가져오기
        method_names = Validation.get_class_methods(CategoryFormat)
        
        # 클래스 인스턴스 생성
        instance = CategoryFormat(root_trigger_id, channel_id, branch_name, key_value, category_name, category_value)
        
        # 모달 호출 
        client.views_open(
            trigger_id= body["trigger_id"]
            ,view= Validation.call_method_by_index(instance, method_names, call_index_to_int)
        )
    except Exception as e :
        # 에러 메시지 발송
        WebClient(token= slack_bolt_bot_token).chat_postMessage(channel= channel_id, text= f"열한시X핸디즈 업무요청 접수 오류 발생, {e}, {root_trigger_id}, <@austin>")

### process 3: 제출 정보 검증 및 슬랙 메시지 발송 - 고객클레임
@app.view("claim_format")
def handel_claim_data(ack, body) :
    ack()
    # 제출 값 가져오기
    raw_private_metadata = body['view']['private_metadata']
    root_trigger_id, channel_id, key_value, category_value = raw_private_metadata.split(',')
    branch_name = body['view']['blocks'][1]['text']['text']
    title = body['view']['title']['text']
    reception_username = body['user']['username']
    reception_user_id = body['user']['id']
    check_in = body['view']['state']['values']['check_in_block']['check_in_action']['selected_date']
    content = body['view']['state']['values']['content_block']['content_action']['value']
    contents_re = re.findall(r'[가-힣A-Za-z0-9]+', content)
    contents_join = ' '.join(contents_re)
    reception_date = datetime.now().strftime("%Y-%m-%d")
    room_ho = body['view']['state']['values']['room_no_block']['room_no_action']['value']
    room_ho_int = re.findall(r'\d+', room_ho)
    room_ho_join = ''.join(room_ho_int)
    building = body['view']['state']['values']['options_building_block']['building_select_action']['selected_option']['text']['text']
    room_change = body['view']['state']['values']['room_change_block']['room_change_action']['selected_option']['text']['text']
    detail_category = body['view']['state']['values']['detail_category_block']['detail_category_action']['selected_option']['text']['text']
    
    try : 
        # 제출한 동, 호수 기반으로 room_no, room_id 찾기
        room_id = Validation.find_room_id(key_value, building, room_ho_join)
        room_no = Validation.find_room_no(key_value, building, room_ho_join)

        if Validation.find_room_id(key_value, building, room_ho_join) == 0 :
            # 객실번호 오류 알림 발송
            response = WebClient(token= slack_bolt_bot_token
            ).chat_postEphemeral(
                channel= channel_id
                ,user= reception_user_id
                ,text= f"객실번호가 올바르지 않습니다. 객실번호 확인 후 다시 접수해주세요.\n\n기존 작성 정보\n● 동: {building}\n● 객실번호: {room_ho_join}\n● 고객 체크인 날짜: {check_in}\n● 객실 변경 여부: {room_change}\n● 접수 내용: {contents_join}"
            )
            
        else :
            # 슬랙 메시지 발송
            response = WebClient(token= slack_bolt_bot_token
            ).chat_postMessage(
                channel= channel_id
                ,text= f"열한시X핸디즈 업무요청 접수 / {title}"
                ,blocks= MessageTemplate.claim_message(title, branch_name, reception_user_id, room_no, check_in, room_change, content, root_trigger_id, detail_category)
            )

            # 제출 정보 DB insert
            original_ts = response['ts']
            cl_cd, branch_id = key_value.split("_")   

            # 데이터테이블 반영
            update_sql = f"""
            insert into client_request_list (
                root_trigger_id, message_ts, reception_username, reception_user_id, reception_date, category, cl_cd, branch_id, room_id, room_no, reception_contents, check_in_claim, room_validation, room_change, requester, claim_category
            ) values (
                '{root_trigger_id}', '{original_ts}', '{reception_username}', '{reception_user_id}', '{reception_date}', '{title}', '{cl_cd}', {branch_id}, {room_id}, '{room_no}', '{contents_join}', '{check_in}', 'valid', '{room_change}', 'handys', '{detail_category}'
            )
            ;
            """
            SqlUtils(host, user, password, db, update_sql).insert_data_value(table= "client_request_list", value= update_sql)

    except Exception as e :
        # 에러 메시지 발송
        WebClient(token= slack_bolt_bot_token).chat_postMessage(channel= channel_id, text= f"열한시X핸디즈 업무요청접수오류 - 고객클레임 접수, {e}, {root_trigger_id}, <@austin>")

### process 3: 제출 정보 검증 및 슬랙 메시지 발송 - 고객클레임 외 청소 미흡
@app.view("poor_format")
def handel_poor_cleaning_data(ack, body) :
    ack()
    # 제출 값 가져오기
    raw_private_metadata = body['view']['private_metadata']
    root_trigger_id, channel_id, key_value, category_value = raw_private_metadata.split(',')
    branch_name = body['view']['blocks'][1]['text']['text']
    title = body['view']['title']['text']
    reception_username = body['user']['username']
    reception_user_id = body['user']['id']
    room_no = body['view']['state']['values']['room_no_block']['room_no_action']['value']
    check_in = body['view']['state']['values']['check_in_block']['check_in_action']['selected_date']
    content = body['view']['state']['values']['content_block']['content_action']['value']
    contents_re = re.findall(r'[가-힣A-Za-z0-9]+', content)
    contents_join = ' '.join(contents_re)
    reception_date = datetime.now().strftime("%Y-%m-%d")
    room_ho = body['view']['state']['values']['room_no_block']['room_no_action']['value']
    room_ho_int = re.findall(r'\d+', room_ho)
    room_ho_join = ''.join(room_ho_int)
    building = body['view']['state']['values']['options_building_block']['building_select_action']['selected_option']['text']['text']
    
    # 동 선택 예외처리 함수
    def building_exception() :
        building = body['view']['state']['values']['options_building_block']['building_select_action']['selected_option']['text']['text']
        if building == '없음' :
            return ''
        else :
            return building + "동-"
    
    try : 
        # 제출한 동, 호수 기반으로 room_no, room_id 찾기
        room_id = Validation.find_room_id(key_value, building, room_ho_join)
        room_no = Validation.find_room_no(key_value, building, room_ho_join)

        if Validation.find_room_id(key_value, building, room_ho_join) == 0 :
            # 슬랙 메시지 발송
            response = WebClient(token= slack_bolt_bot_token
            ).chat_postMessage(
                channel= channel_id
                ,text= f"열한시X핸디즈 업무요청 접수 / {title}"
                ,blocks= MessageTemplate.poor_message(title, branch_name, reception_user_id, building_exception()+room_ho, check_in, content, root_trigger_id)
            )

            # 제출 정보 DB insert
            original_ts = response['ts']
            cl_cd, branch_id = key_value.split("_")

            # 데이터테이블 반영
            update_sql = f"""
            insert into client_request_list (
                root_trigger_id, message_ts, reception_username, reception_user_id, reception_date, category, cl_cd, branch_id, room_id, room_no, reception_contents, check_in_claim, room_validation, requester
            ) values (
                '{root_trigger_id}', '{original_ts}', '{reception_username}', '{reception_user_id}', '{reception_date}', '{title}', '{cl_cd}', {branch_id}, {room_id}, '{building_exception()+room_ho}', '{contents_join}', '{check_in}', 'invalid', 'handys'
            )
            ;
            """
            SqlUtils(host, user, password, db, update_sql).insert_data_value(table= "client_request_list", value= update_sql)
            
            # 객실번호 확인 스레드 전송
            WebClient(token= slack_bolt_bot_token
            ).chat_postMessage(
                channel= channel_id
                ,thread_ts= original_ts
                ,text= "객실번호를 찾지 못했습니다. 다시 확인 후 스레드로 정확한 객실번호를 남겨주세요."
            )
        else :
            # 슬랙 메시지 발송
            response = WebClient(token= slack_bolt_bot_token
            ).chat_postMessage(
                channel= channel_id
                ,text= f"열한시X핸디즈 업무요청 접수 / {title}"
                ,blocks= MessageTemplate.poor_message(title, branch_name, reception_user_id, room_no, check_in, content, root_trigger_id)
            )

            # 제출 정보 DB insert
            original_ts = response['ts']
            cl_cd, branch_id = key_value.split("_")

            # 데이터테이블 반영
            update_sql = f"""
            insert into client_request_list (
                root_trigger_id, message_ts, reception_username, reception_user_id, reception_date, category, cl_cd, branch_id, room_id, room_no, reception_contents, check_in_claim, room_validation, requester
            ) values (
                '{root_trigger_id}', '{original_ts}', '{reception_username}', '{reception_user_id}', '{reception_date}', '{title}', '{cl_cd}', {branch_id}, {room_id}, '{room_no}', '{contents_join}', '{check_in}', 'valid', 'handys'
            )
            ;
            """
            SqlUtils(host, user, password, db, update_sql).insert_data_value(table= "client_request_list", value= update_sql)

    except Exception as e :
        # 에러 메시지 발송
        WebClient(token= slack_bolt_bot_token).chat_postMessage(channel= channel_id, text= f"열한시X핸디즈 업무요청접수오류 - 고객클레임 외 청소 미흡 접수, {e}, {root_trigger_id}, <@austin>")

### process 3: 제출 정보 검증 및 슬랙 메시지 발송 - 하우스맨 업무 요청
@app.view("houseman_format")
def handle_houseman_data(ack, body) :
    ack()
    # 제출 값 가져오기
    raw_private_metadata = body['view']['private_metadata']
    root_trigger_id, channel_id, key_value, category_value = raw_private_metadata.split(',')
    title = body['view']['title']['text']
    branch_name = body['view']['blocks'][1]['text']['text']
    reception_username = body['user']['username']
    reception_user_id = body['user']['id']
    content = body['view']['state']['values']['content_block']['content_action']['value']
    contents_re = re.findall(r'[가-힣A-Za-z0-9]+', content)
    contents_join = ' '.join(contents_re)
    assigned_user = body['view']['state']['values']['users_block']['users_action']['selected_user']
    reception_date = datetime.now().strftime("%Y-%m-%d")
    room_ho = body['view']['state']['values']['room_no_block']['room_no_action']['value']
    room_ho_int = re.findall(r'\d+', room_ho)
    room_ho_join = ''.join(room_ho_int)
    building = body['view']['state']['values']['options_building_block']['building_select_action']['selected_option']['text']['text']

    # 동 선택 예외처리 함수
    def building_exception() :
        building = body['view']['state']['values']['options_building_block']['building_select_action']['selected_option']['text']['text']
        if building == '없음' :
            return ''
        else :
            return building + "동-"
        
    try :
        # 제출한 동, 호수 기반으로 room_no, room_id 찾기
        room_id = Validation.find_room_id(key_value, building, room_ho_join)
        room_no = Validation.find_room_no(key_value, building, room_ho_join)
            
        if Validation.find_room_id(key_value, building, room_ho_join) == 0 :
            # 슬랙 메시지 발송
            response = WebClient(token= slack_bolt_bot_token
            ).chat_postMessage(
                channel= channel_id
                ,text= f"열한시X핸디즈 업무요청 접수 / {title}"
                ,blocks= MessageTemplate.houseman_message(title, assigned_user, branch_name, reception_user_id, building_exception()+room_ho, content, root_trigger_id)
            )
            # 전송 메시지 타임스탬프 획득
            original_ts = response['ts']
            cl_cd, branch_id = key_value.split("_")

            # 데이터테이블 반영
            update_sql = f"""
            insert into client_request_list (
                root_trigger_id, message_ts, reception_username, reception_user_id, reception_date, category, assigned_user_id, cl_cd, branch_id, room_id, room_no, reception_contents, room_validation, requester
            ) values (
                '{root_trigger_id}', '{original_ts}', '{reception_username}', '{reception_user_id}', '{reception_date}', '{title}', '{assigned_user}', '{cl_cd}', {branch_id}, {room_id}, '{building_exception()+room_ho}', '{contents_join}', 'invalid', 'handys'
            )
            ;
            """
            SqlUtils(host, user, password, db, update_sql).insert_data_value(table= "client_request_list", value= update_sql)
            
            # 객실번호 확인 스레드 전송
            WebClient(token= slack_bolt_bot_token
            ).chat_postMessage(
                channel= channel_id
                ,thread_ts= original_ts
                ,text= "객실번호를 찾지 못했습니다. 다시 확인 후 스레드로 정확한 객실번호를 남겨주세요."
            )
        else :
            # 슬랙 메시지 발송
            response = WebClient(token= slack_bolt_bot_token
            ).chat_postMessage(
                channel= channel_id
                ,text= f"열한시X핸디즈 업무요청 접수 / {title}"
                ,blocks= MessageTemplate.houseman_message(title, assigned_user, branch_name, reception_user_id, room_no, content, root_trigger_id)
            )
    
            # 전송 메시지 타임스탬프 획득
            original_ts = response['ts']
            cl_cd, branch_id = key_value.split("_")

            # 데이터테이블 반영
            update_sql = f"""
            insert into client_request_list (
                root_trigger_id, message_ts, reception_username, reception_user_id, reception_date, category, assigned_user_id, cl_cd, branch_id, room_id, room_no, reception_contents, room_validation, requester
            ) values (
                '{root_trigger_id}', '{original_ts}', '{reception_username}', '{reception_user_id}', '{reception_date}', '{title}', '{assigned_user}', '{cl_cd}', {branch_id}, {room_id}, '{room_no}', '{contents_join}', 'valid', 'handys'
            )
            ;
            """
            SqlUtils(host, user, password, db, update_sql).insert_data_value(table= "client_request_list", value= update_sql)

    except Exception as e :
        # 에러 메시지 발송
        WebClient(token= slack_bolt_bot_token).chat_postMessage(channel= channel_id, text= f"열한시X핸디즈 업무요청접수오류 - {title}접수, {e}, {root_trigger_id}, <@austin>")

### process 3: 제출 정보 검증 및 슬랙 메시지 발송 - 딜리버리 업무 요청
@app.view("delivery_format")
def handle_delivery_data(ack, body) :
    ack()
    # 제출 값 가져오기
    raw_private_metadata = body['view']['private_metadata']
    root_trigger_id, channel_id, key_value, category_value = raw_private_metadata.split(',')
    title = body['view']['title']['text']
    branch_name = body['view']['blocks'][1]['text']['text']
    reception_username = body['user']['username']
    reception_user_id = body['user']['id']
    content = body['view']['state']['values']['content_block']['content_action']['value']
    contents_re = re.findall(r'[가-힣A-Za-z0-9]+', content)
    contents_join = ' '.join(contents_re)
    assigned_user = body['view']['state']['values']['users_block']['users_action']['selected_user']
    reception_date = datetime.now().strftime("%Y-%m-%d")
    room_ho = body['view']['state']['values']['room_no_block']['room_no_action']['value']
    room_ho_int = re.findall(r'\d+', room_ho)
    room_ho_join = ''.join(room_ho_int)
    building = body['view']['state']['values']['options_building_block']['building_select_action']['selected_option']['text']['text']

    # 복수 체크 values값 찾기
    def items_parsing() : 
        items_cnt = len(body["view"]["state"]["values"]["items_block"]["items_action"]["selected_options"])
        items_list = []

        for i in range(0, items_cnt) : 
            check_items = body["view"]["state"]["values"]["items_block"]["items_action"]["selected_options"][i]["text"]["text"]
            items_list.append(check_items)
        
        join_str = ','.join(items_list)

        return join_str

    # 동 선택 예외처리 함수
    def building_exception() :
        building = body['view']['state']['values']['options_building_block']['building_select_action']['selected_option']['text']['text']
        if building == '없음' :
            return ''
        else :
            return building + "동-"
        
    try :
        # 제출한 동, 호수 기반으로 room_no, room_id 찾기
        room_id = Validation.find_room_id(key_value, building, room_ho_join)
        room_no = Validation.find_room_no(key_value, building, room_ho_join)
            
        if Validation.find_room_id(key_value, building, room_ho_join) == 0 :
            # 슬랙 메시지 발송
            response = WebClient(token= slack_bolt_bot_token
            ).chat_postMessage(
                channel= channel_id
                ,text= f"열한시X핸디즈 업무요청 접수 / {title}"
                ,blocks= MessageTemplate.delivery_message(title, assigned_user, branch_name, reception_user_id, building_exception()+room_ho, items_parsing(), content, root_trigger_id)
            )
            # 전송 메시지 타임스탬프 획득
            original_ts = response['ts']
            cl_cd, branch_id = key_value.split("_")

            # 데이터테이블 반영
            update_sql = f"""
            insert into client_request_list (
                root_trigger_id, message_ts, reception_username, reception_user_id, reception_date, category, assigned_user_id, cl_cd, branch_id, room_id, room_no, category_items, reception_contents, room_validation, requester
            ) values (
                '{root_trigger_id}', '{original_ts}', '{reception_username}', '{reception_user_id}', '{reception_date}', '{title}', '{assigned_user}', '{cl_cd}', {branch_id}, {room_id}, '{building_exception()+room_ho}', '{items_parsing()}', '{contents_join}', 'invalid', 'handys'
            )
            ;
            """
            SqlUtils(host, user, password, db, update_sql).insert_data_value(table= "client_request_list", value= update_sql)
            
            # 객실번호 확인 스레드 전송
            WebClient(token= slack_bolt_bot_token
            ).chat_postMessage(
                channel= channel_id
                ,thread_ts= original_ts
                ,text= "객실번호를 찾지 못했습니다. 다시 확인 후 스레드로 정확한 객실번호를 남겨주세요."
            )
        else :
            # 슬랙 메시지 발송
            response = WebClient(token= slack_bolt_bot_token
            ).chat_postMessage(
                channel= channel_id
                ,text= f"열한시X핸디즈 업무요청 접수 / {title}"
                ,blocks= MessageTemplate.delivery_message(title, assigned_user, branch_name, reception_user_id, room_no, items_parsing(), content, root_trigger_id)
            )
    
            # 전송 메시지 타임스탬프 획득
            original_ts = response['ts']
            cl_cd, branch_id = key_value.split("_")

            # 데이터테이블 반영
            update_sql = f"""
            insert into client_request_list (
                root_trigger_id, message_ts, reception_username, reception_user_id, reception_date, category, assigned_user_id, cl_cd, branch_id, room_id, room_no, category_items, reception_contents, room_validation, requester
            ) values (
                '{root_trigger_id}', '{original_ts}', '{reception_username}', '{reception_user_id}', '{reception_date}', '{title}', '{assigned_user}', '{cl_cd}', {branch_id}, {room_id}, '{room_no}', '{items_parsing()}', '{contents_join}', 'valid', 'handys'
            )
            ;
            """
            SqlUtils(host, user, password, db, update_sql).insert_data_value(table= "client_request_list", value= update_sql)

    except Exception as e :
        # 에러 메시지 발송
        WebClient(token= slack_bolt_bot_token).chat_postMessage(channel= channel_id, text= f"열한시X핸디즈 업무요청접수오류 - {title}접수, {e}, {root_trigger_id}, <@austin>")

### process 3: 제출 정보 검증 및 슬랙 메시지 발송 - 기타 업무 요청
@app.view("etc_format")
def handle_etc_data(ack, body) :
    ack()
    try : 
        branch_name = body['view']['blocks'][1]['text']['text']
        raw_private_metadata = body['view']['private_metadata']
        root_trigger_id, channel_id, key_value, category_value = raw_private_metadata.split(',')

        # 제출 값 가져오기
        title = body['view']['title']['text']
        reception_username = body['user']['username']
        reception_user_id = body['user']['id']
        content = body['view']['state']['values']['content_block']['content_action']['value']
        contents_re = re.findall(r'[가-힣A-Za-z0-9]+', content)
        contents_join = ' '.join(contents_re)
        assigned_user = body['view']['state']['values']['users_block']['users_action']['selected_user']
        reception_date = datetime.now().strftime("%Y-%m-%d")
        
        # 슬랙 메시지 발송
        response = WebClient(token= slack_bolt_bot_token
        ).chat_postMessage(
            channel= channel_id
            ,text= f"열한시X핸디즈 업무요청 접수 / {title}"
            ,blocks= MessageTemplate.etc_message(title, assigned_user, branch_name, reception_user_id, content, root_trigger_id)
        )
    
        # 전송 메시지 타임스탬프 획득
        original_ts = response['ts']
        cl_cd, branch_id = key_value.split("_")
        
        # 데이터테이블 반영
        update_sql = f"""
        insert into client_request_list (
            root_trigger_id, message_ts, reception_username, reception_user_id, reception_date, category, assigned_user_id, cl_cd, branch_id, reception_contents, requester
        ) values (
            '{root_trigger_id}', '{original_ts}', '{reception_username}', '{reception_user_id}', '{reception_date}', '{title}', '{assigned_user}', '{cl_cd}', {branch_id}, '{contents_join}', 'handys'
        )
        ;
        """
        SqlUtils(host, user, password, db, update_sql).insert_data_value(table= "client_request_list", value= update_sql)

    except Exception as e :
        # 에러 메시지 발송
        WebClient(token= slack_bolt_bot_token).chat_postMessage(channel= channel_id, text= f"열한시X핸디즈 업무요청접수오류 - 기타업무요청접수, {e}, {root_trigger_id}, <@austin>")

### process 4: 결과 작성 리액션 이벤트
@app.event("reaction_added")
def handle_reaction(ack, event, client) :
    ack()
    try : 
        # 이벤트 데이터에서 필요한 정보 추출
        reaction = event["reaction"]
        channel_id = event["item"]["channel"]
        thread_ts = event["item"]["ts"]
        reaction_user_id = event['user']

        # 메시지 본문 가져오기
        response = client.conversations_history(
            channel=channel_id,
            latest=thread_ts,
            limit=1,
            inclusive=True
        )
        title_text = response['messages'][0]['text']

        # 리액션 유효성 체크
        if "열한시X핸디즈 업무요청 접수" in title_text and reaction == "완료_2" :

            # 원 메시지에서 root_trigger_id 찾기
            def root_trigger_id_parsing() : 
                blocks = response["messages"][0]["blocks"]

                for index, block in enumerate(blocks) :
                    if block["type"] == "context" : 
                        root_trigger_id = response["messages"][0]["blocks"][index]["elements"][0]["text"]
                
                return root_trigger_id
                    
            # 메시지에 버튼 추가
            WebClient(token= slack_bolt_bot_token
            ).chat_postMessage(
                channel= channel_id
                ,thread_ts= thread_ts
                ,text= "업무처리완료"
                ,attachments= MessageTemplate.result_reaction_message(root_trigger_id_parsing())
            )
            
            # 완료 결과 DB 적재
            submit_date = datetime.now().strftime("%Y-%m-%d")

            # 데이터테이블 반영
            update_sql = f"""
            insert into client_request_process_result (
                root_trigger_id, message_ts, submit_date, submit_username
            ) values (
                '{root_trigger_id_parsing()}', '{thread_ts}', '{submit_date}', '{reaction_user_id}'
            )
            ;
            """
            SqlUtils(host, user, password, db, update_sql).insert_data_value(table= "client_request_process_result", value= update_sql)

        else :
            pass 
    
    except Exception as e :
        # 에러 메시지 발송
        WebClient(token= slack_bolt_bot_token).chat_postMessage(channel= channel_id, text= f"열한시X핸디즈 업무요청접수오류 - 결과작성리액션, {e}, <@austin>")

### process 5: 결과 작성 모달창 오픈
@app.action("result_modal")
def handle_result_modal(ack, body, client) :
    ack()
    try : 
        trigger_id = body["trigger_id"]
        channel_id = body["channel"]["id"]
        thread_ts = body["original_message"]["ts"]
        root_trigger_id = body["original_message"]["attachments"][0]["fallback"]

        # 모달 트리거
        client.views_open(
            trigger_id= trigger_id
            ,view= ModalFormat(root_trigger_id,channel_id,'','').result_format(thread_ts)
        )
    except Exception as e :
        # 에러 메시지 발송
        WebClient(token= slack_bolt_bot_token).chat_postMessage(channel= channel_id, text= f"열한시X핸디즈 업무요청접수오류 - 결과작성창오픈, {e}, <@austin>")

### process 6: 결과 작성 내용 스레드 전송 및 DB 적재
@app.view("result_data")
def handle_result_inform(ack, body) : 
    ack()
    try : 
        # 모달이 생성된 메시지의 channel_id와 thread_ts 가져오기 (private_metadata)
        private_metadata = body["view"]["private_metadata"]
        root_trigger_id, channel_id, key_value, thread_ts = private_metadata.split(',')
        
        # 제출 데이터 파싱
        submit_user_id = body['user']['id']
        submit_user = body['user']['username']
        reception_user_id = Validation.find_receipt_user(root_trigger_id= root_trigger_id)
        submit_date = datetime.now().strftime("%Y-%m-%d")
        content = body["view"]["state"]["values"]["content_block"]["content_action"]["value"]
        contents_re = re.findall(r'[가-힣A-Za-z0-9]+', content)
        contents_join = ' '.join(contents_re)

        # 작성 내용 스레드 발송
        WebClient(token= slack_bolt_bot_token
        ).chat_postMessage(
            channel=channel_id,
            thread_ts=thread_ts,  # 스레드에 답글로 작성
            text= 
                f"<@{reception_user_id}>, 접수하신 건의 답변 내용입니다.\n\n"
                +f">{content}\n\n"
                +f"작성자: <@{submit_user_id}>"
        )

        # SQS send message
        AwsUtils.send_sqs_message(
            messageTitle= "handys_request_result",
            messageBody= {
                "root_trigger_id" : f'{root_trigger_id}',
                "message_ts" : f'{thread_ts}',
                "submit_date": f'{submit_date}',
                "submit_username" : f'{submit_user}',
                "submit_contents" : f'{contents_join}'
            }
        )

        # Airflow Dag Run API 요청
        dag_id = "sqs_trigger_request_bot"
        user = os.environ['AIRFLOW_USERNAME']
        password = os.environ['AIRFLOW_PASSWORD']
        UTC = timezone(timedelta(hours=-9))
        run_time_kst = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        run_time_utc = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        
        
        url = f"https://airflow.11h.kr/api/v1/dags/{dag_id}/dagRuns"

        headers = {
            "Content-Type": "application/json",
            "Accept" : "application/json",
        }

        body = {
            "dag_run_id": f"airflow_api_sqs_{run_time_kst}",
            "logical_date": f"{run_time_utc}",
            "data_interval_start": f"{run_time_kst}",
            "data_interval_end": f"{run_time_kst}",
            "conf": { },
            "note": "airflow-api-sqs-trigger"
        }

        response = requests.post(
            url= url, 
            headers= headers, 
            data= json.dumps(body), 
            auth= HTTPBasicAuth(username= user, password= password)
        )
        print(f"Airflow Dag Run: {response}")

    except Exception as e :
        # 에러 메시지 발송
        WebClient(token= slack_bolt_bot_token).chat_postMessage(channel= channel_id, text= f"열한시X핸디즈 업무요청접수오류 - 결과작성내용스레드전송, {e}, {root_trigger_id}, <@austin>")

# Socket 실행 main 함수
if __name__ == "__main__" : 
    SocketModeHandler(app, slack_bolt_app_token).start()