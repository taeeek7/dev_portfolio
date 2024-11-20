FROM apache/airflow:2.9.3-python3.12

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
      vim \
      wget \
      unzip \
      gnupg2 \
      curl \
      unzip \
      # Chrome 의존성 패키지
      libnss3 \
      libgbm1 \
      libasound2 \
      libxss1 \
      xvfb \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# 시간대 설정 중 상호작용 방지
ENV DEBIAN_FRONTEND=noninteractive

# Chrome 설치
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# Chrome 버전 확인 및 일치하는 ChromeDriver 다운로드
RUN CHROME_VERSION=$(google-chrome --version | grep -oP 'Chrome \K[0-9]+') \
    && wget -q "https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/$CHROME_VERSION.0.6723.0/linux64/chromedriver-linux64.zip" -O /tmp/chromedriver.zip \
    && unzip /tmp/chromedriver.zip -d /usr/local/bin/ \
    && mv /usr/local/bin/chromedriver-linux64/chromedriver /usr/local/bin/ \
    && rm -rf /tmp/chromedriver.zip /usr/local/bin/chromedriver-linux64 \
    && chmod +x /usr/local/bin/chromedriver

# Xvfb 설정
ENV DISPLAY=:99

COPY ./.env ./.env
RUN source .env

USER airflow

COPY requirements.txt /
RUN pip3 install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

COPY ./dags /opt/airflow/dags
COPY ./services /opt/airflow/services