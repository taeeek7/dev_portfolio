from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

class SlackUtils :  
    # 생성자
    def __init__(self, *args) :
        self.slack_token = args[0]
        self.slack_channel = args[1]
    
    # 슬랙메시지발송 함수 
    def send_messages(self, text) : 

        ### 슬랙 API 및 메시지 발송 변수 설정 ###
        client_slack = WebClient(token=self.slack_token)

        try : 
            client_slack.chat_postMessage(
                    channel= self.slack_channel
                    ,text=  text 
            )
                
        except SlackApiError as e :
                assert e.response["error"]

    # slack Block-Kit 발송 함수
    def send_block_kit(self, text, blocks) : 

        ### 슬랙 API 및 메시지 발송 변수 설정 ###
        client_slack = WebClient(token=self.slack_token)

        try : 
            response= client_slack.chat_postMessage(
                    channel= self.slack_channel
                    ,text=  text
                    ,blocks= blocks 
            )
            return response
                
        except SlackApiError as e :
            assert e.response["error"]

    # 스레드(reply) 발송 함수
    def send_threads(self, thread_ts, text) : 

        ### 슬랙 API 및 메시지 발송 변수 설정 ###
        client_slack = WebClient(token=self.slack_token)

        try : 
            response = client_slack.chat_postMessage(
                    channel= self.slack_channel
                    ,thread_ts= thread_ts
                    ,text=  text
            )
            return response
                
        except SlackApiError as e :
            assert e.response["error"]

    # slack Block-Kit 발송 함수
    def send_attachment_kit(self, thread_ts, attachments) : 

        ### 슬랙 API 및 메시지 발송 변수 설정 ###
        client_slack = WebClient(token=self.slack_token)

        try : 
            response = client_slack.chat_postMessage(
                    channel= self.slack_channel
                    ,thread_ts= thread_ts
                    ,attachments= attachments 
            )
            return response
                
        except SlackApiError as e :
            assert e.response["error"]