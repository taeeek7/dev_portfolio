# 베이스 이미지로 Python 3.9 사용
FROM python:3.12-slim

# Python 버전과 pip 버전을 미리 업그레이드
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc && \
    pip3 install --upgrade pip

# requirements.txt를 복사하고 필요한 라이브러리 설치
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# 환경변수 파일을 컨테이너로 복사
COPY .env .

# 애플리케이션 소스 코드를 복사
COPY . .

# 환경 변수를 설정 (출력 버퍼링을 비활성화)
ENV PYTHONUNBUFFERED=1

# 애플리케이션 실행 명령어 (여기서는 슬랙봇 실행을 가정)
CMD ["python", "app.py"]
