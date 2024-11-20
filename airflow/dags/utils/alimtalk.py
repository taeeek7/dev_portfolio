import json
import os
import hashlib
import hmac
import base64
import requests
import time
import urllib

base_uri = os.getenv("ALIMTALK_BASE_URI")
alimtalk_url = os.getenv("ALIMTALK_URL")
secret_key = os.getenv("ALIMTALK_SECRET_KEY")

class AlimtalkUtils : 
    # 생성자
    def __init__(self, *args) :
        self.access_key = args[0]
        self.secret_key = args[1]

    # 헤더값 필수 signature 코드 리턴
    def	make_signature(self, uri, method, timestamp):

        access_key = self.access_key
        secret_key = str(self.secret_key).encode('UTF-8')

        uri = uri
        method = method
        
        message = (method + " " + uri + "\n" + timestamp + "\n" + access_key).encode('UTF-8')
        signingKey = base64.b64encode(hmac.new(secret_key, message, digestmod=hashlib.sha256).digest())

        return signingKey.decode('UTF-8')

    # 템플릿 조회 및 규격 확인 함수
    def search_template(self, channel_id, template_code) :
        timestamp = str(int(time.time() * 1000))
        channelId = urllib.parse.quote(channel_id)
        
        get_param = f"channelId={channelId}&templateCode={template_code}"
        get_uri = f"{base_uri}/templates?{get_param}"
        signature = AlimtalkUtils.make_signature(self, uri= get_uri, method= "GET", timestamp= timestamp)
        
        url = f"{alimtalk_url}{get_uri}"

        # 요청 헤더 설정
        headers = {
            "Content-Type": "application/json; charset=UTF-8"
            ,"x-ncp-apigw-timestamp": timestamp
            ,"x-ncp-iam-access-key": f"{self.access_key}"
            ,"x-ncp-apigw-signature-v2": signature
        }

        # GET 요청 보내기
        response = requests.get(url, headers=headers)
        response_text = json.loads(response.text)

        return response_text

    # 알림톡 전송
    def send_alimtalk(self, body) : 
        timestamp = str(int(time.time() * 1000))
        uri = f"{base_uri}/messages"
        signature = AlimtalkUtils.make_signature(self, uri= uri, method= "POST", timestamp= timestamp)
        
        url = f"{alimtalk_url}{uri}"
        
        # 요청 헤더 설정
        headers = {
            "Content-Type": "application/json; charset=UTF-8"
            ,"x-ncp-apigw-timestamp": timestamp
            ,"x-ncp-iam-access-key": f"{self.access_key}"
            ,"x-ncp-apigw-signature-v2": signature
        }

        body = body

        # POST 요청 보내기
        response = requests.post(url, headers=headers, json= body)
        response_text = json.loads(response.text)

        return response_text