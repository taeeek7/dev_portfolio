from dotenv import load_dotenv
import boto3, os, json 

# 환경변수 로드 
load_dotenv()

class AwsUtils :
    def send_sqs_message(messageTitle, messageBody) :
        sqs_client = boto3.client(
            "sqs",
            aws_access_key_id= os.getenv("AWS_ACCESS_KEY"),              # Access Key ID
            aws_secret_access_key= os.getenv("AWS_SECRET_ACCESS_KEY"),       # Secret Access Key
            region_name= "ap-northeast-2",
        )

        data = {
            "messageTitle" : messageTitle,
            "messageBody" : [
                messageBody
            ]
        }
        # Python 객체를 JSON 문자열로 변환
        serialize_data = json.dumps(data)
        
        response = sqs_client.send_message(
            QueueUrl=f'{os.getenv("SQS_URL")}/RequestBot-SQS',
            MessageBody= serialize_data,
        )
        print(f"Message ID: {response['MessageId']}")

    def receive_sqs_message() :
        sqs_client = boto3.client(
            "sqs",
            aws_access_key_id= os.getenv("AWS_ACCESS_KEY"),              # Access Key ID
            aws_secret_access_key= os.getenv("AWS_SECRET_ACCESS_KEY"),       # Secret Access Key
            region_name= "ap-northeast-2",
        )

        response = sqs_client.receive_message(
            QueueUrl= f'{os.getenv("SQS_URL")}/RequestBot-SQS.fifo',
            MaxNumberOfMessages = 5,
            # WaitTimeSeconds= 10  # 메시지가 도착할 때까지 기다릴 시간 (Long Polling)
        )
        messages = response.get("Messages", [])
        
        for message in messages:
            receipt_handler = message['ReceiptHandle']
            message_id = message['MessageId']

            # 메시지 삭제
            sqs_client.delete_message(
                QueueUrl= f'{os.getenv("SQS_URL")}/RequestBot-SQS.fifo',
                ReceiptHandle= receipt_handler
            )
            print(f"Received And Deleted Message: {message_id}")
            
            return message
        else :
            print("No messages found")
            return 


    # def receive_sqs_message(self, sqs_url) :
    #     aws_conn = BaseHook.get_connection(self.aws_conn_id)
    #     sqs_client = boto3.client(
    #         'sqs', 
    #         aws_access_key_id= aws_conn.login,              # Access Key ID
    #         aws_secret_access_key= aws_conn.password,       # Secret Access Key
    #         region_name= self.region_name,
    #     )
    #     response = sqs_client.receive_message(
    #         QueueUrl= sqs_url,
    #         MaxNumberOfMessages = 5,
    #         # WaitTimeSeconds= 10  # 메시지가 도착할 때까지 기다릴 시간 (Long Polling)
    #     )
    #     messages = response.get("Messages", [])
    #     for message in messages:
    #         receipt_handler = message['ReceiptHandle']
    #         message_id = message['MessageId']
            
    #         # 메시지 삭제
    #         sqs_client.delete_message(
    #             QueueUrl= sqs_url,
    #             ReceiptHandle= receipt_handler
    #         )
    #         print(f"Received And Deleted Message: {message_id}")
            
    #         return message
    #     else :
    #         print("No messages found")
    #         return 
    


# # Secrets Manager 클라이언트 생성
# secrets_client = boto3.client('secretsmanager', region_name='your-region')

# # 비밀에서 자격 증명 가져오기
# secret_name = "your-secret-name"
# response = secrets_client.get_secret_value(SecretId=secret_name)
# secret = eval(response['SecretString'])

# # 가져온 자격 증명을 사용하여 SQS 클라이언트 생성
# sqs_client = boto3.client(
#     'sqs',
#     aws_access_key_id=secret['AWS_ACCESS_KEY_ID'],
#     aws_secret_access_key=secret['AWS_SECRET_ACCESS_KEY'],
#     region_name='your-region'
# )

# # SQS 작업 수행
# response = sqs_client.send_message(
#     QueueUrl='https://sqs.your-region.amazonaws.com/your-account-id/your-queue-name',
#     MessageBody='Hello from Docker!'
# )
# print(f"Message ID: {response['MessageId']}")
