from dotenv import load_dotenv
from datetime import datetime
import os
from SqlUtils import SqlUtils
from validation import Validation
  
# 환경변수 import 
load_dotenv()
host = os.getenv("N_HOST")
user = os.getenv("N_USER")
password = os.getenv("N_PASSWORD")
db = os.getenv("N_DB")

# 접수 카테고리 모달 format_name 추출
def category_list() :
    df = SqlUtils(host, user, password, db, 
             """
            select 
                distinct category
            from client_request_category_item
            where 
                status in ('active') 
                and requester = 'handys'
            order by category
            ;
            """
            ).extract_db()
    rename = {
         0: 'category'
    }
    df.rename(columns= rename, inplace= True)
    
    return df

# 카테고리 이이템 리스트 추출
def category_item(category) :
    df = SqlUtils(host, user, password, db, 
            f"select item, id from client_request_category_item where category = '{category}' and status = 'active' ;").extract_db()
    return df 

# Modal 양식 클래스
class ModalFormat :
    # 생성자
    def __init__(self, *args) :
        self.root_trigger_id = args[0]
        self.channel_id = args[1]
        self.branch_name = args[2]
        self.key_value = args[3]

    # 초기 접수 모달창 양식 (카테고리, 지점명 선택)
    def title_format(self) :
        df = SqlUtils(host, user, password, db, 
                "select name, concat(cl_cd,'_',branch_id) as key_value from branch where cl_cd = 'H0001' and name not like '%테스트%' and name not like '%종료%' order by name ;").extract_db()
        
        ## 모달창 json
        modal = {
            "type": "modal",
            "callback_id" : "modal_category_format",
            "title": {
                "type": "plain_text",
                "text": "📬 핸디즈 >> 열한시 업무요청 접수",
                "emoji": True
            },
            "submit": {
                "type": "plain_text",
                "text": "작성",
                "emoji": True
            },
            "close": {
                "type": "plain_text",
                "text": "취소",
                "emoji": True
            },
        }

        ## 선택 블락 json
        blocks = []
        section = {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "안녕하세요. 핸디즈 >> 열한시 업무요청 접수봇입니다. \n\n 접수 카테고리와 지점을 선택하고, 작성을 누르시면 접수 양식이 생성됩니다. \n\n 내용 작성 후 제출해주세요."
                    }
        }
        blocks.append(section)

        # 카테고리 옵션 for-loop
        options_category = []
        for i in range(0,len(category_list())) :
            values = {
                "text": {
                    "type": "plain_text",
                    "text": f"{category_list().iloc[i,0]}",
                    "emoji": True
                },
                "value": f"{category_list().iloc[i,0]}"
            }
            options_category.append(values)
        
        element_category = {
            "type": "input",
            "block_id": "select_category",
            "element": {
                "type": "static_select",
                "placeholder": {
                    "type": "plain_text",
                    "text": "카테고리를 선택해주세요.",
                    "emoji": True
                },
                "options" : options_category,
                "action_id": "category_select_action"
            },
            "label": {
                "type": "plain_text",
                "text": "카테고리",
                "emoji": True
            }
        }
        blocks.append(element_category)

        # 지점명 옵션 for-loop
        options_branch = []
        for i in range(0,len(df)) :
            values  = {
                "text": {
                    "type": "plain_text",
                    "text": f"{df.iloc[i,0]}",
                    "emoji": True
                },
                "value": f"{df.iloc[i,1]}"
            }
            options_branch.append(values)

        element_branch = {
            "type": "input",
            "block_id": "select_branch",
            "element": {
                "type": "static_select",
                "placeholder": {
                    "type": "plain_text",
                    "text": "지점을 선택해주세요.",
                    "emoji": True
                },
                "options" : options_branch,
                "action_id": "branch_select_action"
            },
            "label": {
                "type": "plain_text",
                "text": "지점명",
                "emoji": True
            }
        }
        blocks.append(element_branch)

        ## block json 병합
        blocks_json = {
            "blocks" : blocks
        }

        ## private metadata
        private_metadata = {
            "private_metadata": f"{self.channel_id}"
        }
        
        # json 총 병합
        merged_data = {**modal, **blocks_json, **private_metadata}

        return merged_data
    
    # 결과 작성 모달창 양식
    def result_format(self, thread_ts) : 
        ## 모달창 json
        modal = {
            "type": "modal",
            "callback_id" : "result_data",
            "title": {
                "type": "plain_text",
                "text": "✅ 결과 작성 양식",
                "emoji": True
            },
            "submit": {
                "type": "plain_text",
                "text": "제출",
                "emoji": True
            },
            "close": {
                "type": "plain_text",
                "text": "취소",
                "emoji": True
            },
        }

        ## 선택 블락 json
        blocks = []
        section = {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "요청 내용에 대한 처리 결과를 작성해주세요."
                    }
        }
        blocks.append(section)

        # 작성내용 block
        content_block = {
            "type": "input",
            "block_id": "content_block",
            "element": {
                "type": "plain_text_input",
                "multiline": True,
                "action_id": "content_action"
            },
            "label": {
                "type": "plain_text",
                "text": "결과 내용 작성",
                "emoji": True
            }
        }
        blocks.append(content_block)

        ## block json 병합
        blocks_json = {
            "blocks" : blocks
        }
        
        ## private metadata
        private_metadata = {
            "private_metadata": f"{self.root_trigger_id},{self.channel_id},{self.key_value},{thread_ts}"
        }
        
        # json 총 병합
        merged_data = {**modal, **blocks_json, **private_metadata}

        return merged_data
    
    # 결과 작성 권한 경고 양식
    def submit_check_format(self, category_name, category_value) : 
        ## 모달창 json
        modal = {
            "type": "modal",
            "callback_id" : "submit_check",
            "title": {
                "type": "plain_text",
                "text": "❓작성 내용 오류",
                "emoji": True
            },
            "submit": {
                "type": "plain_text",
                "text": "재작성",
                "emoji": True
            },
            "close": {
                "type": "plain_text",
                "text": "취소",
                "emoji": True
            },
        }

        ## 선택 블락 json
        blocks = []
        section = {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"{self.branch_name} 지점의 객실번호가 아닙니다.\n\n객실번호를 확인 후 다시 작성해주세요."
                    }
        }
        blocks.append(section)

        ## block json 병합
        blocks_json = {
            "blocks" : blocks
        }
        
        ## private metadata
        private_metadata = {
            "private_metadata": f"{self.root_trigger_id},{self.channel_id},{self.branch_name},{self.key_value},{category_name},{category_value}"
        }
        
        # json 총 병합
        merged_data = {**modal, **blocks_json, **private_metadata}

        return merged_data


class CategoryFormat :
     # 생성자
    def __init__(self, *args) :
        self.root_trigger_id = args[0]
        self.channel_id = args[1]
        self.branch_name = args[2]
        self.key_value = args[3]
        self.category_name = args[4]
        self.category_value = args[5]
    
    # 고객클레임 접수 양식
    def a_claim_format(self) :
        today = datetime.now().strftime("%Y-%m-%d")

        ## 모달창 json
        modal = {
            "type": "modal",
            "callback_id" : "claim_format",
            "title": {
                "type": "plain_text",
                "text": f"{self.category_name}",
                "emoji": True
            },
            "submit": {
                "type": "plain_text",
                "text": "제출",
                "emoji": True
            },
            "close": {
                "type": "plain_text",
                "text": "취소",
                "emoji": True
            },
        }

        ## 선택 블락 json
        blocks = []
        section = {
            "type": "section",
            "text": {
                "type": "mrkdwn"
                ,"text": "아래 양식을 작성하신 후 제출 버튼을 눌러주세요.\n\n" 
                        + "※ 안내메세지를 반드시 읽고 따라주세요.\n\n"
                        + "1️⃣ 정확한 클레임 상황을 알기 위해 접수된 슬랙 메세지 스레드로 *증빙사진* 을 남겨주세요.\n\n"
                        + "2️⃣ 현재 창은 *정산에 반영되는* 고객 클레임 접수 양식입니다.\n\n"
                        + "🔗 <https://www.notion.so/11clock/12d11c2e8b0280898832df7901c20638|링크>에 표시된 카테고리를 확인하여 접수해주세요.\n\n"
                        + "(※ 게스트 클레임건이 아닌 경우 고객클레임 외 청소 미흡 피드백으로 접수해주세요.)"
            }
        }
        blocks.append(section)

        ## 선택 지점명 재확인
        branch_section = {
            "type": "header",
            "block_id": "branch_name_block",
            "text": {
                "type": "plain_text",
                "text": f"{self.branch_name}",
                "emoji": True
            }
        }
        blocks.append(branch_section)

        # 카테고리 옵션 for-loop
        options_building = []
        for i in range(0,len(Validation.find_room_building(self.key_value))) :
            values = {
                "text": {
                    "type": "plain_text",
                    "text": f"{Validation.find_room_building(self.key_value).iloc[i,2]}",
                    "emoji": True
                },
                "value": f"{Validation.find_room_building(self.key_value).iloc[i,2]}"
            }
            options_building.append(values)
        
        element_building = {
            "type": "input",
            "block_id": "options_building_block",
            "element": {
                "type": "static_select",
                "placeholder": {
                    "type": "plain_text",
                    "text": "접수 객실의 동을 선택해주세요.",
                    "emoji": True
                },
                "options" : options_building,
                "action_id": "building_select_action"
            },
            "label": {
                "type": "plain_text",
                "text": "동 선택",
                "emoji": True
            }
        }
        blocks.append(element_building)

        room_no_block = {
			"type": "input",
            "block_id" : "room_no_block",
			"element": {
				"type": "plain_text_input",
				"action_id": "room_no_action"
			},
			"label": {
				"type": "plain_text",
				"text": "객실번호 (숫자만 입력하세요)",
				"emoji": True
			}
        }
        blocks.append(room_no_block)

        # 체크인날짜 block
        check_in_block = {
            "type": "input",
            "block_id": "check_in_block",
            "element": {
                "type": "datepicker",
                "initial_date": f"{today}",
                "placeholder": {
                    "type": "plain_text",
                    "text": "날짜 선택",
                    "emoji": True
                },
                "action_id": "check_in_action"
            },
            "label": {
                "type": "plain_text",
                "text": "고객 체크인 날짜",
                "emoji": True
            }
        }
        blocks.append(check_in_block)

        room_change_block = {
            "type": "input",
            "block_id": "room_change_block",
            "element": {
                "type": "static_select",
                "placeholder": {
                    "type": "plain_text",
                    "text": "객실 변경 여부를 선택해주세요.",
                    "emoji": True
                },
                "options" : [
                    {
						"text": {
							"type": "plain_text",
							"text": "Y",
							"emoji": True
						},
						"value": "value-0"
					},
					{
						"text": {
							"type": "plain_text",
							"text": "N",
							"emoji": True
						},
						"value": "value-1"
					},
                ],
                "action_id": "room_change_action"
            },
            "label": {
                "type": "plain_text",
                "text": "객실 변경 여부",
                "emoji": True
            }
        }
        blocks.append(room_change_block)

        # 세부 카테고리 block
        detail_category_block = {
            "type": "input",
            "block_id": "detail_category_block",
            "element": {
                "type": "static_select",
                "placeholder": {
                    "type": "plain_text",
                    "text": "고객클레임 세부 유형을 선택해주세요.",
                    "emoji": True
                },
                "options" : [
					{
						"text": {
							"type": "plain_text",
							"text": "냄새 및 악취",
						},
						"value": "value-0"
					},
                    {
						"text": {
							"type": "plain_text",
							"text": "린넨 및 침구류 오염",
						},
						"value": "value-1"
					},
                    {
						"text": {
							"type": "plain_text",
							"text": "먼지 및 머리카락",
						},
						"value": "value-2"
					},
                    {
						"text": {
							"type": "plain_text",
							"text": "비품 누락 및 배치 불량",
						},
						"value": "value-3"
					},
                    {
						"text": {
							"type": "plain_text",
							"text": "사용 흔적 또는 남은 음식물",
						},
						"value": "value-4"
					},
                    {
						"text": {
							"type": "plain_text",
							"text": "시설물 관리 미흡",
						},
						"value": "value-5"
					},
					{
						"text": {
							"type": "plain_text",
							"text": "욕실 청소 미흡",
						},
						"value": "value-6"
					},
                    {
						"text": {
							"type": "plain_text",
							"text": "주방 청소 미흡",
						},
						"value": "value-7"
					},
                    {
						"text": {
							"type": "plain_text",
							"text": "청소 미흡(일반)",
						},
						"value": "value-8"
					},
				],
                "action_id": "detail_category_action"
            },
            "label": {
                "type": "plain_text",
                "text": "고객클레임 세부 유형",
                "emoji": True
            }
        }
        blocks.append(detail_category_block)

        # 접수 내용 block
        content_block = {
            "type": "input",
            "block_id": "content_block",
            "element": {
                "type": "plain_text_input",
                "multiline": True,
                "action_id": "content_action"
            },
            "label": {
                "type": "plain_text",
                "text": "접수 내용",
                "emoji": True
            }
        }
        blocks.append(content_block)

        ## block json 병합
        blocks_json = {
            "blocks" : blocks
        }
        
        ## private metadata
        private_metadata = {
            "private_metadata": f"{self.root_trigger_id},{self.channel_id},{self.key_value},{self.category_value}"
        }
        
        # json 총 병합
        merged_data = {**modal, **blocks_json, **private_metadata}

        return merged_data

    # 고객클레임 외 청소 미흡 피드백 접수 양식
    def b_poor_cleaning_format(self) :
        today = datetime.now().strftime("%Y-%m-%d")

        ## 모달창 json
        modal = {
            "type": "modal",
            "callback_id" : "poor_format",
            "title": {
                "type": "plain_text",
                "text": f"{self.category_name}",
                "emoji": True
            },
            "submit": {
                "type": "plain_text",
                "text": "제출",
                "emoji": True
            },
            "close": {
                "type": "plain_text",
                "text": "취소",
                "emoji": True
            },
        }

        ## 선택 블락 json
        blocks = []
        section = {
            "type": "section",
            "text": {
                "type": "mrkdwn"
                ,"text": "아래 양식을 작성하신 후 제출 버튼을 눌러주세요.\n\n" 
                        + "※ 안내메세지를 반드시 읽고 따라주세요.\n\n"
                        + "1️⃣ 정확한 청소미흡 상황을 알기 위해 접수된 슬랙 메세지 스레드로 *증빙사진* 을 남겨주세요.\n\n"
                        + "2️⃣ 현재 창은 *정산에 미반영되는* 고객클레임 외 청소 미흡 피드백 접수 양식입니다.\n\n"
                        + "(※ 게스트 클레임건인 경우 고객클레임으로 접수해주세요.)"
            }
        }
        blocks.append(section)

        ## 선택 지점명 재확인
        branch_section = {
            "type": "header",
            "block_id": "branch_name_block",
            "text": {
                "type": "plain_text",
                "text": f"{self.branch_name}",
                "emoji": True
            }
        }
        blocks.append(branch_section)

        # 카테고리 옵션 for-loop
        options_building = []
        for i in range(0,len(Validation.find_room_building(self.key_value))) :
            values = {
                "text": {
                    "type": "plain_text",
                    "text": f"{Validation.find_room_building(self.key_value).iloc[i,2]}",
                    "emoji": True
                },
                "value": f"{Validation.find_room_building(self.key_value).iloc[i,2]}"
            }
            options_building.append(values)
        
        element_building = {
            "type": "input",
            "block_id": "options_building_block",
            "element": {
                "type": "static_select",
                "placeholder": {
                    "type": "plain_text",
                    "text": "접수 객실의 동을 선택해주세요.",
                    "emoji": True
                },
                "options" : options_building,
                "action_id": "building_select_action"
            },
            "label": {
                "type": "plain_text",
                "text": "동 선택",
                "emoji": True
            }
        }
        blocks.append(element_building)

        room_no_block = {
            "type": "input",
            "block_id" : "room_no_block",
            "element": {
                "type": "plain_text_input",
                "action_id": "room_no_action"
            },
            "label": {
                "type": "plain_text",
                "text": "객실번호 (숫자만 입력하세요)",
                "emoji": True
            }
        }
        blocks.append(room_no_block)

        # 체크인날짜 block
        check_in_block = {
            "type": "input",
            "block_id": "check_in_block",
            "element": {
                "type": "datepicker",
                "initial_date": f"{today}",
                "placeholder": {
                    "type": "plain_text",
                    "text": "빌견일 선택",
                    "emoji": True
                },
                "action_id": "check_in_action"
            },
            "label": {
                "type": "plain_text",
                "text": "발견일",
                "emoji": True
            }
        }
        blocks.append(check_in_block)

        # 접수 내용 block
        content_block = {
            "type": "input",
            "block_id": "content_block",
            "element": {
                "type": "plain_text_input",
                "multiline": True,
                "action_id": "content_action"
            },
            "label": {
                "type": "plain_text",
                "text": f"미흡내용 / 요청사항",
                "emoji": True
            }
        }
        blocks.append(content_block)

        ## block json 병합
        blocks_json = {
            "blocks" : blocks
        }
        
        ## private metadata
        private_metadata = {
            "private_metadata": f"{self.root_trigger_id},{self.channel_id},{self.key_value},{self.category_value}"
        }
        
        # json 총 병합
        merged_data = {**modal, **blocks_json, **private_metadata}

        return merged_data
    
    # 하우스맨 업무 접수 양식
    def c_houseman_format(self) : 
        ## 모달창 json
        modal = {
            "type": "modal",
            "callback_id" : "houseman_format",
            "title": {
                "type": "plain_text",
                "text": f"{self.category_name}",
                "emoji": True
            },
            "submit": {
                "type": "plain_text",
                "text": "제출",
                "emoji": True
            },
            "close": {
                "type": "plain_text",
                "text": "취소",
                "emoji": True
            },
        }

        ## 선택 블락 json
        blocks = []
        section = {
            "type": "section",
            "text": {
                "type": "plain_text",
                "text": "아래 항목을 선택 및 작성 후 제출 버튼을 눌러주세요.\n\n",
                "emoji": True
            }
        }
        blocks.append(section)

        ## 선택 지점명 재확인
        branch_section = {
            "type": "header",
            "block_id": "branch_name_block",
            "text": {
                "type": "plain_text",
                "text": f"{self.branch_name}",
                "emoji": True
            }
        }
        blocks.append(branch_section)

        # 카테고리 옵션 for-loop
        options_building = []
        for i in range(0,len(Validation.find_room_building(self.key_value))) :
            values = {
                "text": {
                    "type": "plain_text",
                    "text": f"{Validation.find_room_building(self.key_value).iloc[i,2]}",
                    "emoji": True
                },
                "value": f"{Validation.find_room_building(self.key_value).iloc[i,2]}"
            }
            options_building.append(values)
        
        element_building = {
            "type": "input",
            "block_id": "options_building_block",
            "element": {
                "type": "static_select",
                "placeholder": {
                    "type": "plain_text",
                    "text": "접수 객실의 동을 선택해주세요.",
                    "emoji": True
                },
                "options" : options_building,
                "action_id": "building_select_action"
            },
            "label": {
                "type": "plain_text",
                "text": "동 선택",
                "emoji": True
            }
        }
        blocks.append(element_building)

        room_no_block = {
			"type": "input",
            "block_id" : "room_no_block",
			"element": {
				"type": "plain_text_input",
				"action_id": "room_no_action"
			},
			"label": {
				"type": "plain_text",
				"text": "객실번호 (숫자만 입력하세요)",
				"emoji": True
			}
        }
        blocks.append(room_no_block)

        # 접수 내용 block
        content_block = {
            "type": "input",
            "block_id": "content_block",
            "element": {
                "type": "plain_text_input",
                "multiline": True,
                "action_id": "content_action"
            },
            "label": {
                "type": "plain_text",
                "text": "요청 내용",
                "emoji": True
            }
        }
        blocks.append(content_block)

        user_block = {
            "type": "section",
            "block_id": "users_block",
            "text": {
                "type": "mrkdwn",
                "text": "담당자를 선택해주세요."
            },
            "accessory": {
                "type": "users_select",
                "placeholder": {
                    "type": "plain_text",
                    "text": "Select a user",
                    "emoji": True
                },
                "action_id": "users_action"
            }
        }
        blocks.append(user_block)

        ## block json 병합
        blocks_json = {
            "blocks" : blocks
        }
        
        ## private metadata
        private_metadata = {
            "private_metadata": f"{self.root_trigger_id},{self.channel_id},{self.key_value},{self.category_value}"
        }
        
        # json 총 병합
        merged_data = {**modal, **blocks_json, **private_metadata}

        return merged_data
    
    # 회수/배달 업무 접수 양식
    def d_delivery_format(self) : 
        ## 모달창 json
        modal = {
            "type": "modal",
            "callback_id" : "delivery_format",
            "title": {
                "type": "plain_text",
                "text": f"{self.category_name}",
                "emoji": True
            },
            "submit": {
                "type": "plain_text",
                "text": "제출",
                "emoji": True
            },
            "close": {
                "type": "plain_text",
                "text": "취소",
                "emoji": True
            },
        }

        ## 선택 블락 json
        blocks = []
        section = {
            "type": "section",
            "text": {
                "type": "plain_text",
                "text": "아래 항목을 선택 및 작성 후 제출 버튼을 눌러주세요.\n\n",
                "emoji": True
            }
        }
        blocks.append(section)

        ## 선택 지점명 재확인
        branch_section = {
            "type": "header",
            "block_id": "branch_name_block",
            "text": {
                "type": "plain_text",
                "text": f"{self.branch_name}",
                "emoji": True
            }
        }
        blocks.append(branch_section)

        # 카테고리 옵션 for-loop
        options_building = []
        for i in range(0,len(Validation.find_room_building(self.key_value))) :
            values = {
                "text": {
                    "type": "plain_text",
                    "text": f"{Validation.find_room_building(self.key_value).iloc[i,2]}",
                    "emoji": True
                },
                "value": f"{Validation.find_room_building(self.key_value).iloc[i,2]}"
            }
            options_building.append(values)
        
        element_building = {
            "type": "input",
            "block_id": "options_building_block",
            "element": {
                "type": "static_select",
                "placeholder": {
                    "type": "plain_text",
                    "text": "접수 객실의 동을 선택해주세요.",
                    "emoji": True
                },
                "options" : options_building,
                "action_id": "building_select_action"
            },
            "label": {
                "type": "plain_text",
                "text": "동 선택",
                "emoji": True
            }
        }
        blocks.append(element_building)

        room_no_block = {
			"type": "input",
            "block_id" : "room_no_block",
			"element": {
				"type": "plain_text_input",
				"action_id": "room_no_action"
			},
			"label": {
				"type": "plain_text",
				"text": "객실번호 (숫자만 입력하세요)",
				"emoji": True
			}
        }
        blocks.append(room_no_block)

        # 카테고리 옵션 for-loop
        options_category = []
        for i in range(0,len(category_item(self.category_name))) :
            values = {
                "text": {
                    "type": "plain_text",
                    "text": f"{category_item(self.category_name).iloc[i,0]}",
                    "emoji": True
                },
                "value": f"{self.category_name}_{category_item(self.category_name).iloc[i,1]}"
            }
            options_category.append(values)
        
        element_category = {
            "type": "input",
            "block_id": "items_block",
            "element": {
                "type": "multi_static_select",
                "placeholder": {
                    "type": "plain_text",
                    "text": f"업무 종류를 선택해주세요.",
                    "emoji": True
                },
                "options" : options_category,
                "action_id": "items_action"
            },
            "label": {
                "type": "plain_text",
                "text": "업무 종류",
                "emoji": True
            }
        }
        blocks.append(element_category)

        # 접수 내용 block
        content_block = {
            "type": "input",
            "block_id": "content_block",
            "element": {
                "type": "plain_text_input",
                "multiline": True,
                "action_id": "content_action"
            },
            "label": {
                "type": "plain_text",
                "text": "요청 내용",
                "emoji": True
            }
        }
        blocks.append(content_block)

        user_block = {
            "type": "section",
            "block_id": "users_block",
            "text": {
                "type": "mrkdwn",
                "text": "담당자를 선택해주세요."
            },
            "accessory": {
                "type": "users_select",
                "placeholder": {
                    "type": "plain_text",
                    "text": "Select a user",
                    "emoji": True
                },
                "action_id": "users_action"
            }
        }
        blocks.append(user_block)

        ## block json 병합
        blocks_json = {
            "blocks" : blocks
        }
        
        ## private metadata
        private_metadata = {
            "private_metadata": f"{self.root_trigger_id},{self.channel_id},{self.key_value},{self.category_value}"
        }
        
        # json 총 병합
        merged_data = {**modal, **blocks_json, **private_metadata}

        return merged_data

    # 기타업무요청 접수 양식
    def e_etc_format(self) : 
        ## 모달창 json
        modal = {
            "type": "modal",
            "callback_id" : "etc_format",
            "title": {
                "type": "plain_text",
                "text": f"{self.category_name}",
                "emoji": True
            },
            "submit": {
                "type": "plain_text",
                "text": "제출",
                "emoji": True
            },
            "close": {
                "type": "plain_text",
                "text": "취소",
                "emoji": True
            },
        }

        ## 선택 블락 json
        blocks = []
        section = {
            "type": "section",
            "text": {
                "type": "plain_text",
                "text": "요청 내용 작성 및 담당자 선택 후 제출 버튼을 눌러주세요.\n",
                "emoji": True
            }
        }
        blocks.append(section)

        ## 선택 지점명 재확인
        branch_section = {
            "type": "header",
            "block_id": "branch_name_block",
            "text": {
                "type": "plain_text",
                "text": f"{self.branch_name}",
                "emoji": True
            }
        }
        blocks.append(branch_section)

        # 접수 내용 block
        content_block = {
            "type": "input",
            "block_id": "content_block",
            "element": {
                "type": "plain_text_input",
                "multiline": True,
                "action_id": "content_action"
            },
            "label": {
                "type": "plain_text",
                "text": "요청 내용",
                "emoji": True
            }
        }
        blocks.append(content_block)

        user_block = {
            "type": "section",
            "block_id": "users_block",
            "text": {
                "type": "mrkdwn",
                "text": "담당자를 선택해주세요."
            },
            "accessory": {
                "type": "users_select",
                "placeholder": {
                    "type": "plain_text",
                    "text": "Select a user",
                    "emoji": True
                },
                "action_id": "users_action"
            }
        }
        blocks.append(user_block)

        ## block json 병합
        blocks_json = {
            "blocks" : blocks
        }
        
        ## private metadata
        private_metadata = {
            "private_metadata": f"{self.root_trigger_id},{self.channel_id},{self.key_value},{self.category_value}"
        }
        
        # json 총 병합
        merged_data = {**modal, **blocks_json, **private_metadata}

        return merged_data