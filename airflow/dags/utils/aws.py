from airflow.hooks.base import BaseHook
import boto3

class AwsUtils :
    def __init__(self, *args) :
        self.aws_conn_id = args[0]
        self.region_name = args[1]

    def receive_sqs_message(self, sqs_url) :
        aws_conn = BaseHook.get_connection(self.aws_conn_id)
        sqs_client = boto3.client(
            "sqs", 
            aws_access_key_id= aws_conn.login,              # Access Key ID
            aws_secret_access_key= aws_conn.password,       # Secret Access Key
            region_name= self.region_name,
        )
        response = sqs_client.receive_message(
            QueueUrl= sqs_url,
            MaxNumberOfMessages = 5,
            # WaitTimeSeconds= 10  # 메시지가 도착할 때까지 기다릴 시간 (Long Polling)
        )
        messages = response.get("Messages", [])
        for message in messages:
            receipt_handler = message["ReceiptHandle"]
            message_id = message["MessageId"]
            
            # 메시지 삭제
            sqs_client.delete_message(
                QueueUrl= sqs_url,
                ReceiptHandle= receipt_handler
            )
            print(f"Received And Deleted Message: {message_id}")
            
            return message
        else :
            print("No messages found")
            return 
    
    def multi_receive_sqs_message(self, sqs_url) :
        aws_conn = BaseHook.get_connection(self.aws_conn_id)
        sqs_client = boto3.client(
            "sqs", 
            aws_access_key_id= aws_conn.login,              # Access Key ID
            aws_secret_access_key= aws_conn.password,       # Secret Access Key
            region_name= self.region_name,
        )
        all_messages = []
        while True:
            response = sqs_client.receive_message(
                QueueUrl=sqs_url,
                MaxNumberOfMessages=10,  # SQS allows a maximum of 10 messages per request
                WaitTimeSeconds=0  # Set to 0 for short polling
            )
            
            messages = response.get("Messages", [])
            if not messages:
                print("No more messages in the queue")
                break
            
            for message in messages:
                receipt_handle = message["ReceiptHandle"]
                message_id = message["MessageId"]
                
                # 메시지 저장
                all_messages.append(message)

                # 메시지 삭제
                sqs_client.delete_message(
                    QueueUrl=sqs_url,
                    ReceiptHandle=receipt_handle
                )
                print(f"Received And Deleted Message: {message_id}")
        
        print(f"Total messages received and processed: {len(all_messages)}")
        return all_messages