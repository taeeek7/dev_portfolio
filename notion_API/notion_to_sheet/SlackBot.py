from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


# 슬랙봇 발송 함수 
# 변수항목: 슬랙봇 토큰, 슬랙채널ID, 발송메시지
def send_slackbot(slack_token, slack_channel, text) :
    
    ### 슬랙 API 및 메시지 발송 변수 설정 ###
    slack_token = slack_token
    client_slack = WebClient(token=slack_token)

    try:
        response_slack = client_slack.chat_postMessage(
            channel= slack_channel ,   
            text=  text 
        )
    except SlackApiError as e:
        assert e.response["error"]
