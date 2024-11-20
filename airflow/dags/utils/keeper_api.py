import requests
import json 

class KeeperApiUtils :
    def __init__(self, *args) :
        self.url = args[0]
    # 포인트 조정 API
    def post_set_point(self, endPoint, memberKeeperId, pointModifyCode, pointModifyComment, workPoint) :
        # API 엔드포인트 URL
        url = f"{self.url}/{endPoint}"

        # 요청 헤더 설정
        headers = {
            "memberAdminId": "1" ,
            "level": "90",
            "Content-Type": "application/json;charset=UTF-8",
        }
            
        # 요청 본문 데이터 설정
        data = {
            "memberKeeperId": memberKeeperId
            , "pointModifyCode": pointModifyCode
            , "pointModifyComment": pointModifyComment
            , "workPoint": workPoint
        }
            
        # POST 요청 보내기
        responseAPI = requests.post(url, headers= headers, data=json.dumps(data))
        result = json.loads(responseAPI.text)

        return result 
    
    # 수기티켓등록 API
    def post_set_complete(self, endPoint, clCd, branchId, roomId, ticketCode, searchDate, memberKeeperId, completeCode, completeComment, depth2Rate, depth3Rate, keeperRate, benefitCost) :
        # API 엔드포인트 URL
        url = f"{self.url}/{endPoint}"

        # 요청 헤더 설정
        headers = {
            "memberAdminId": "1" ,
            "level": "90",
            "Content-Type": "application/json;charset=UTF-8",
        }

        # 요청 본문 데이터 설정
        data = {
            "clCd": clCd,
            "branchId": branchId,
            "roomId": roomId,
            "ticketCode": ticketCode,
            "searchDate": searchDate,
            "memberKeeperId": memberKeeperId,
            "completeCode": completeCode,
            "completeComment": completeComment,
            "depth2Rate": depth2Rate,
            "depth3Rate": depth3Rate,
            "keeperRate": keeperRate,
            "benefitCost": benefitCost
        }

        # POST 요청 보내기
        responseAPI = requests.post(url, headers= headers, data=json.dumps(data))
        result = json.loads(responseAPI.text)

        return result
    
    def post_update_complete(self, endPoint, orderNo, completeCode, completeComment, depth2Rate, depth3Rate, keeperRate, benefitCost) :
        # API 엔드포인트 URL
        url = f"{self.url}/{endPoint}"

        # 요청 헤더 설정
        headers = {
            "memberAdminId": "1" ,
            "level": "90",
            "Content-Type": "application/json;charset=UTF-8",
        }

        # 요청 본문 데이터 설정
        data = {
            "orderNo": orderNo
            , "completeCode": completeCode
            , "completeComment": completeComment
            , "depth2Rate": depth2Rate
            , "depth3Rate": depth3Rate
            , "keeperRate": keeperRate
            , "benefitCost" : benefitCost
        }

        # POST 요청 보내기
        responseAPI = requests.post(url, headers= headers, data=json.dumps(data))
        result = json.loads(responseAPI.text)

        return result

    def post_insert_ticket(self, endPoint, clCd, branchId, roomId, ticketCode, emergencyCode, emergencyComment, searchDate, memberKeeperId) :
        # API 엔드포인트 URL
        url = f"{self.url}/{endPoint}"

        # 요청 헤더 설정
        headers = {
            "memberAdminId": "1" ,
            "level": "90",
            "Content-Type": "application/json;charset=UTF-8",
        }

        # 요청 본문 데이터 설정
        data = {
            "clCd" : clCd,
            "branchId" : branchId,
            "roomId" : roomId,
            "ticketCode" : ticketCode,
            "emergencyCode" : emergencyCode,
            "emergencyComment" : emergencyComment,
            "searchDate" : searchDate,
            "memberKeeperId": memberKeeperId,
        }

        # POST 요청 보내기
        responseAPI = requests.post(url, headers= headers, data=json.dumps(data))
        result = json.loads(responseAPI.text)

        return result