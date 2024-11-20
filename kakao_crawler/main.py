from dotenv import load_dotenv
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time, os
import pandas as pd

class KakaoService :
    def __init__(self, url):
        #크롬드라이브 옵션 설정
        chrome_options = webdriver.ChromeOptions()
        # chrome_options.add_argument('headless')
        chrome_options.add_experimental_option("prefs", {"profile.default_content_setting_values.notifications": 1})
        chrome_options.add_experimental_option("detach", True)
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options) 
        self.wait = WebDriverWait(self.driver, 20)
        self.url = url

    def open_web_and_login(self, login_id, password):
        
        #크롬 페이지 열기
        self.driver.get(self.url)
        print("Open web browser")

        #로그인 
        id_input = self.driver.find_element(By.ID, "loginId--1")
        id_input.send_keys(login_id)

        pw_input = self.driver.find_element(By.ID, "password--2")
        pw_input.send_keys(password)

        try: 
            login_button = self.driver.find_element(By.XPATH, "//*[@id='mainContent']/div/div/form/div[4]/button[1]")
            login_button.click()
            print("login success")
        except : 
            print("login failure")
    
    def two_factor_handler(self) :
        time.sleep(10)
        # 2단계 인증 입력 필드 대기
        auth_field = self.wait.until(EC.presence_of_element_located((By.ID, "passcode--6")))
        
        # 사용자에게 인증 코드 입력 요청
        auth_code = input("2단계 인증 코드를 입력하세요: ")
        
        # 인증 코드 입력 및 제출
        auth_field.send_keys(auth_code)
        submit_button = self.driver.find_element(By.XPATH, "//*[@id='mainContent']/div/div/form/div[4]/button")
        submit_button.click()
    
    def quit_browser(self) :
        self.driver.quit()
        print("quit browser")
    
    # 채팅 목록 페이지 오픈
    def open_chat_page(self, channel_id) :
        time.sleep(1)
        self.driver.get(f"https://center-pf.kakao.com/{channel_id}/chats")
        print(f"{channel_id} page open")
    
    # 채팅방 id 크롤링
    def chat_id_crawler(self, channel_id, branch_name) :
        # 페이지 로딩 대기시간 
        time.sleep(2)

        # chat_id 담을 리스트  
        ids = []
        html = self.driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        
        # 페이지 전체에서 label 태그 찾기
        labels = soup.find_all('label')
        for label in labels:
            if 'for' in label.attrs and 'chat-select-' in label['for']:
                chat_id = label['for'].replace("chat-select-", "")
                # 각 행의 데이터를 리스트로 추가
                ids.append([branch_name, channel_id, chat_id])
                
            else :
                pass
        
        if len(ids) == 0 :
            print("추출된 chat_id가 없어 엑셀 파일을 생성하지 않았습니다.")
        else :
            # 데이터프레임 생성
            df = pd.DataFrame(ids, columns=['branch_name', 'channel_id', 'chat_id'])
            # 엑셀 파일로 저장
            df.to_excel(f'/Users/tin/kakao-crawler/chat_ids/{branch_name}.{channel_id}_chat_ids.xlsx', index=True)
            
            print("엑셀 파일 생성 완료")

        return

    # 채팅방 오픈
    def open_dialog_page(self, channel_id, chat_id) : 
        time.sleep(1)
        self.driver.get(f"https://center-pf.kakao.com/{channel_id}/chats/{chat_id}")
        print(f"{chat_id} page open")

    # 대화내용 크롤링
    def chat_dialog_crawler(self, branch_name, channel_id, chat_id) : 
        # 페이지 로딩 대기시간 
        time.sleep(2)
        
        # 채팅창 요소 찾기
        chat_container = self.driver.find_element(By.CLASS_NAME, "fake_scroll")
        
        # 이전 대화 내용이 모두 로드될 때까지 스크롤
        prev_height = None
        while True:
            # 현재 스크롤 높이 저장
            current_height = self.driver.execute_script("return arguments[0].scrollHeight", chat_container)
            
            # 최상단으로 스크롤
            self.driver.execute_script("arguments[0].scrollTop = 0", chat_container)
            time.sleep(1.5)  # 로딩 대기 시간 늘림
            
            # 새로운 높이 확인
            new_height = self.driver.execute_script("return arguments[0].scrollHeight", chat_container)
            
            # 높이가 같으면 (더 이상 로드될 내용이 없으면) 종료
            if prev_height == new_height:
                break
                
            prev_height = new_height
        
        # 전체 HTML 가져오기
        html = self.driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        chat_data = []

        # 채팅 내용 파싱
        chat_items = soup.find_all('div', class_='item_chat')
        last_sender = None  # 직전 발신자 저장용

        for chat in chat_items:
            # 보낸 사람 찾기 (새로운 발신자가 있는 경우에만 업데이트)
            sender_elem = chat.find('strong', class_='txt_user')
            if sender_elem:
                last_sender = sender_elem.text.strip()
        
            # 메시지 찾기
            message_elem = chat.find('p', class_='txt_chat')
            if message_elem:
                message = message_elem.get_text('\n', strip=True)
                
                chat_data.append({
                    'channel_id' : channel_id, 
                    'chat_id': chat_id,
                    'sender': last_sender,
                    'message': message
                })
        
        # DataFrame 생성 및 엑셀 저장
        df = pd.DataFrame(chat_data)
        excel_file = f'/Users/tin/kakao-crawler/chat_history/{branch_name}.{channel_id}_chat_history.xlsx'
        
        # 기존 엑셀 파일이 있으면 읽어서 새 데이터 추가
        if os.path.exists(excel_file):
            df_existing = pd.read_excel(excel_file)
            df_combined = pd.concat([df_existing, df], ignore_index=True)
        else:
            df_combined = df
        
        # 엑셀 파일로 저장
        df_combined.to_excel(excel_file, index=False)
        print(f"채팅 내용이 {excel_file}에 추가되었습니다.")

        return

def open_branch_channel_id() :
    df = pd.read_excel('branch_channel_id.xlsx')
    return df 

def open_chat_ids_file(branch_name, channel_id) :
    try : 
        df = pd.read_excel(f'/Users/tin/kakao-crawler/chat_ids/{branch_name}.{channel_id}_chat_ids.xlsx')
        return df
    except :
        print("엑셀파일을 찾을 수 없습니다.")
        return None
    

# 채팅방 id 크롤링 핸들러
def crawling_chat_id_handler() :
    login_id = "idddd@11c.kr"
    password = "passwordddd"
    df = open_branch_channel_id()
    
    # 객체생성
    kakao_client = KakaoService(url= "https://center-pf.kakao.com/profiles")
    
    # 로그인 / 2차인증
    kakao_client.open_web_and_login(login_id= login_id, password= password)
    kakao_client.two_factor_handler()

    for i in range(0, len(df)) :
        branch_name = df.iloc[i,0]
        channel_id = df.iloc[i,1]

        kakao_client.open_chat_page(channel_id= channel_id)
        
        kakao_client.chat_id_crawler(
            channel_id= channel_id, 
            branch_name= branch_name
        )
    
    kakao_client.quit_browser()


# 대화 내용 크롤링 핸들러 
def crawling_dialog_handler() :
    login_id = "austin@11c.kr"
    password = "#rud9751"
    df = open_branch_channel_id()

    # 객체생성
    kakao_client = KakaoService(url= "https://center-pf.kakao.com/profiles")
    
    # 로그인 / 2차인증
    kakao_client.open_web_and_login(login_id= login_id, password= password)
    time.sleep(20)
    kakao_client.two_factor_handler()

    for i in range(0, len(df)) :
        branch_name = df.iloc[i,0]
        channel_id = df.iloc[i,1]
        
        chat_id_file = open_chat_ids_file(
            branch_name= branch_name,
            channel_id= channel_id
        )

        if chat_id_file is None :
            print(f"{branch_name} chat_ids 파일 없음")

        else :
            for i in range(0, len(chat_id_file)) :
                chat_branch_name = chat_id_file.iloc[i,1]
                chat_channel_id = chat_id_file.iloc[i,2]
                chat_chat_id = chat_id_file.iloc[i,3]

                kakao_client.open_dialog_page(
                    channel_id= chat_channel_id,
                    chat_id= chat_chat_id
                )    
            
                kakao_client.chat_dialog_crawler(
                    branch_name= chat_branch_name,
                    channel_id= chat_channel_id,
                    chat_id= chat_chat_id
                )

    kakao_client.quit_browser()

    return 


# main 함수
if __name__ == "__main__" : 
    crawling_chat_id_handler()
    crawling_dialog_handler()
    

