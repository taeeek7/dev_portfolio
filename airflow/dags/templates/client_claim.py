from utils.sql import SqlUtils
import pandas as pd 

class GetData :
    # 운영사별 슬랙 채널 추출 
    def get_slack_channel() :
        source_data = SqlUtils.get_source_data(
            conn_id= "cleanops",
            sql= """
            select 
                *
                ,case
                    when company_name = "올데이즈" then "ben" 
                    when company_name = "열한시전남지사" then "genie"
                    when company_name = "프리앤워커" then "leo"
                    when company_name = "에스앤씨" then "hole"
                    when company_name = "태태클린" then "tash"
                    else '' end as represent
            from slack_ops_bot_channel
            ;
            """
        )
        df = pd.DataFrame(source_data)
        return df 
    
    # 포인트 감점 대상자 추출
    def get_claim_person(company_name) :
        source_data = SqlUtils.get_source_data(
            conn_id= "cleanops",
            sql= f"""
            select 
                b.name as branch 
                ,mk.name as keeper_name
                ,case 
                    when ccl.grade_calculate = 50 then '브론즈'
                    when ccl.grade_calculate = 70 then '실버'
                    when ccl.grade_calculate = 72 then '골드'
                    when ccl.grade_calculate = 75 then '플래티넘'
                    when ccl.grade_calculate = 80 then '다이아몬드'
                    end as grade
                ,r.room_no
                ,DATE_FORMAT(ccl.end_at, "%m/%d") as cleaning_date
                ,ccl.reception_contents
                ,ccl.inspect_score
                ,case 
                    when ccl.inspect_score <= 3 then 0
                    else ccl.point * (-2) 
                    end as pay_point
                ,ccl.member_keeper_id
                ,ccl.root_trigger_id
                ,b.company_name
                ,sobc.test_ch_id
                ,ccl.order_no
            from client_claim_list as ccl
            inner join client as cl
                on ccl.cl_cd = cl.cl_cd
            inner join branch as b 
                on ccl.cl_cd = b.cl_cd
                and ccl.branch_id = b.branch_id
            inner join room as r 
                on ccl.cl_cd = r.cl_cd
                and ccl.branch_id = r.branch_id
                and ccl.room_id = r.room_id
            inner join member_keeper as mk 
                on ccl.member_keeper_id = mk.member_keeper_id
            inner join ticket_type as tt 
                on ccl.ticket_code = tt.code
            inner join slack_ops_bot_channel as sobc
                on b.company_name = sobc.company_name
            where 
                b.company_name = "{company_name}"
                and ccl.reception_date = DATE_FORMAT(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), '%Y-%m-%d 00:00:00')
            ;
            """
        )
        df = pd.DataFrame(source_data)
        if len(df) == 0 :
            return df 
        else :
            rename = {
                0: "branch"
                ,1: "keeper_name"
                ,2: "grade"
                ,3: "room_no"
                ,4: "cleaning_date"
                ,5: "reception_contents"
                ,6: "inspect_score"
                ,7: "pay_point"
                ,8: "member_keeper_id"
                ,9: "root_trigger_id"
                ,10: "company_name"
                ,11: "ch_id"
                ,12: "order_no"
            }
            df.rename(columns= rename, inplace= True)
            
            return df

class MessageTemplate :
    # 포인트 감점 대상자 슬랙 알림 포맷
    def common_section(represent) : 
        blocks = []

        title_section = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"<!channel>\n*⚠️[고객클레임] 포인트 감점 확인 요청*"
            }
        }
        sub_section = {
            "type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "```아래 고객 클레임 리스트를 확인하여 주세요. (차감 포인트 : -300P)\n클레임 소명 사유가 있는 경우, 기본 점수 차감 버튼을 누른 후 댓글로 소명 사유를 작성해주세요.\n포인트 차감은 금일 17시 이후 적용됩니다.\n\n표기 예시)\n 지점\n 클리닝 담당키퍼 / 객실호수(클리닝 수행일) / 포인트 차감 (- np) / 클레임 사유```"
			}
        }
        blocks.append(title_section)
        blocks.append(sub_section)

        return blocks
    
    # 포인트 감점 대상자 상세 리스트 포맷 
    def penalty_list_section(company_name) :
        blocks = []

        df = GetData.get_claim_person(company_name= company_name)
        for i in range (0, len(df)) :
            normal_text_format = f"{df.iloc[i,1]} 키퍼님 고객클레임 접수 내용 확인되어 -300점 차감 예정입니다."
            hold_text_format = f"{df.iloc[i,1]} 키퍼님 검수점수 {df.iloc[i,6]}점으로 기본점수 {df.iloc[i,7]}점 차감 예정입니다."
            exemption_text_format = f"{df.iloc[i,1]} 키퍼님 소명 사유 확인되어 점수 차감 보류합니다."
            charge_text_format = f"{df.iloc[i,12]} 해당 오더건 고객사 청구 비율 0% 반영 예정입니다."
            keeper_section = {
                "type": "section",
                "block_id": f"keeper_section_{i}",
                "text": {
                    "type": "mrkdwn",
                    "text": f"{df.iloc[i,0]}\n\n {df.iloc[i,1]}({df.iloc[i,2]}) / {df.iloc[i,3]}({df.iloc[i,4]}) → 포인트 감점 (-300p) / {df.iloc[i,5]}"
                }
            }
            trigger_section = {
                "type": "context",
                "block_id": f"trigger_section_{i}",
                "elements": [
                    {
                        "type": "plain_text",
                        "text": f"접수건 확인 ⬇️⬇️ <아래 메시지 키로 검색 가능> \n\n {df.iloc[i,9]}",
                        "emoji": True
                    }
                ]
            }
            button_action = {
                "type": "actions",
                "block_id": f"button_section_{i}",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "정상 차감",
                            "emoji": True
                        },
                        "value": f"{df.iloc[i,9]},{df.iloc[i,7]},{normal_text_format}",
                        "action_id": "normal_action"
                    },
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "기본 점수 차감",
                            "emoji": True
                        },
                        "value": f"{df.iloc[i,9]},{df.iloc[i,7]},{hold_text_format}",
                        "action_id": "hold_action"
                    },
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "점수 차감 보류",
                            "emoji": True
                        },
                        "value": f"{df.iloc[i,9]},{df.iloc[i,7]},{exemption_text_format}",
                        "action_id": "exemption_action"
                    },
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "청구 비율 조정",
                            "emoji": True
                        },
                        "value": f"{df.iloc[i,9]},{df.iloc[i,12]},{charge_text_format}",
                        "action_id": "charge_action"
                    },
                ],
            }
            
            divider = {
                "type": "divider",
                "block_id": f"{df.iloc[i,9]}"
            }
            
            blocks.append(keeper_section)
            blocks.append(trigger_section)
            blocks.append(button_action)
            blocks.append(divider)

        return blocks
