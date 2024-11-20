class MessageTemplate :   
    def __init__(self) :
        pass

    # 고객클레임 접수 슬랙 메시지 발송 템플릿
    def claim_message(category, branch_name, user, room_no, check_in, room_change, content, root_trigger_id, detail_category) :
        blocks = []
        title = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*_📬 핸디즈 >> 열한시 업무요청 접수_*"
            }
        }
        sub_title = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text":  f">카테고리: {category}"
            }
        }
        divide = {
            "type": "divider"
        }
        main = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f">접수자: <@{user}>\n"
                        +f">지점: {branch_name}\n"
                        +f">객실 번호: {room_no}\n"
                        +f">고객 체크인 날짜: {check_in}\n"
                        +f">객실 변경 여부: {room_change}\n"
                        +f">세부 유형: {detail_category}\n"
                        +f">접수 내용: {content}\n"
            }
        }
        trigger = {
            "type": "context",
            "elements": [
                {
                    "type": "plain_text",
                    "text": f"{root_trigger_id}",
                    "emoji": True
                }
            ]
        }
        blocks.append(title)
        blocks.append(sub_title)
        blocks.append(divide)
        blocks.append(main)
        blocks.append(trigger)

        return blocks

    # 고객클레임 외 청소 미흡 접수 슬랙 메시지 발송 템플릿
    def poor_message(category, branch_name, user, room_no, check_in, content, root_trigger_id) :
        blocks = []
        title = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*_📬 핸디즈 >> 열한시 업무요청 접수_*"
            }
        }
        sub_title = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text":  f">카테고리: {category}"
            }
        }
        divide = {
            "type": "divider"
        }
        main = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f">접수자: <@{user}>\n"
                        +f">지점: {branch_name}\n"
                        +f">객실 번호: {room_no}\n"
                        +f">발생 날짜: {check_in}\n"
                        +f">미흡내용 / 요청사항: {content}\n"
            }
        }
        trigger = {
            "type": "context",
            "elements": [
                {
                    "type": "plain_text",
                    "text": f"{root_trigger_id}",
                    "emoji": True
                }
            ]
        }
        blocks.append(title)
        blocks.append(sub_title)
        blocks.append(divide)
        blocks.append(main)
        blocks.append(trigger)

        return blocks

    # 하우스맨 업무 요청 슬랙 메시지 발송 템플릿
    def houseman_message(category, assigned_user, branch_name, receipt_user, room_no, content, root_trigger_id) :
        blocks = []
        title = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*_📬 핸디즈 >> 열한시 업무요청 접수_*"
            }
        }
        sub_title = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text":  f">카테고리: {category}\n"
                        +f">담당자: <@{assigned_user}>\n"   
            }
        }
        divide = {
            "type": "divider"
        }
        main = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f">접수자: <@{receipt_user}>\n"
                        +f">지점: {branch_name}\n"
                        +f">객실 번호: {room_no}\n"
                        +f">요청 내용: {content}\n"
            }
        }
        trigger = {
            "type": "context",
            "elements": [
                {
                    "type": "plain_text",
                    "text": f"{root_trigger_id}",
                    "emoji": True
                }
            ]
        }
        blocks.append(title)
        blocks.append(sub_title)
        blocks.append(divide)
        blocks.append(main)
        blocks.append(trigger)

        return blocks

    # 딜리버리 업무 요청 슬랙 메시지 발송 템플릿
    def delivery_message(category, assigned_user, branch_name, receipt_user, room_no, category_item, content, root_trigger_id) :
        blocks = []
        title = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*_📬 핸디즈 >> 열한시 업무요청 접수_*"
            }
        }
        sub_title = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text":  f">카테고리: {category}\n"
                        +f">담당자: <@{assigned_user}>\n"   
            }
        }
        divide = {
            "type": "divider"
        }
        main = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f">접수자: <@{receipt_user}>\n"
                        +f">지점: {branch_name}\n"
                        +f">객실 번호: {room_no}\n"
                        +f">업무 종류: {category_item}\n"
                        +f">요청 내용: {content}\n"
            }
        }
        trigger = {
            "type": "context",
            "elements": [
                {
                    "type": "plain_text",
                    "text": f"{root_trigger_id}",
                    "emoji": True
                }
            ]
        }
        blocks.append(title)
        blocks.append(sub_title)
        blocks.append(divide)
        blocks.append(main)
        blocks.append(trigger)

        return blocks

    # 기타 업무 요청 슬랙 메시지 발송 템플릿
    def etc_message(category, assigned_user, branch_name, receipt_user, content, root_trigger_id) :
        blocks = []
        title = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*_📬 핸디즈 >> 열한시 업무요청 접수_*"
            }
        }
        sub_title = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text":  f">카테고리: {category}\n"
                        +f">담당자: <@{assigned_user}>\n"   
            }
        }
        divide = {
            "type": "divider"
        }
        main = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f">접수자: <@{receipt_user}>\n"
                        +f">지점: {branch_name}\n"
                        +f">요청 내용: {content}\n"
            }
        }
        trigger = {
            "type": "context",
            "elements": [
                {
                    "type": "plain_text",
                    "text": f"{root_trigger_id}",
                    "emoji": True
                }
            ]
        }
        blocks.append(title)
        blocks.append(sub_title)
        blocks.append(divide)
        blocks.append(main)
        blocks.append(trigger)

        return blocks

    # 결과 작성 리액션 메시지
    def result_reaction_message(root_trigger_id_parsing) :
        attchments = []
        contents = {
            "text": "업무 처리가 완료되었습니다. \n\n 추가 코멘트를 작성하시려면 버튼을 눌러주세요.",
            "fallback": f"{root_trigger_id_parsing}",
            "callback_id": "result_modal",
            "color": "#6ed3b3",
            "attachment_type": "default",
            "actions": [
                {
                    "name": "open_modal",
                    "text": "결과 코멘트 작성",
                    "type": "button",
                    "value": "open_modal"
                }
            ]           
        }
        attchments.append(contents)
        return attchments

    # 접수창 오픈 메시지 양식
    def welcome_message() :
        blocks = []
        text_section = {
            "type": "rich_text",
            "elements": [
                {
                    "type": "rich_text_section",
                    "elements": [
                        {
                            "type": "text",
                            "text": "업무요청양식 작성을 위해 버튼을 눌러주세요!"
                        }
                    ]
                }
            ]
        }
        button_section = {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "작성하기",
                        "emoji": True
                    },
                    "value": "click_me_123",
                    "action_id": "open_title_modal"
                }
            ]
        }
        blocks.append(text_section)
        blocks.append(button_section)

        return blocks