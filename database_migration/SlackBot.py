from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

class SlackBot :
        
        # 생성자
        def __init__(self, *args) :
                self.slack_token = args[0]
                self.slack_channel = args[1]
        
        # 슬랙메시지발송 함수 
        def send_messages(self, text) : 

                ### 슬랙 API 및 메시지 발송 변수 설정 ###
                slack_token = self.slack_token
                client_slack = WebClient(token=self.slack_token)

                try : 
                        response_slack = client_slack.chat_postMessage(
                        channel= self.slack_channel ,   
                        text=  text 
                        )
                        
                except SlackApiError as e :
                        assert e.response["error"]





                