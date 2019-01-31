FROM python:3.6
WORKDIR /usr/src/app

RUN mkdir mcnulty logs data
COPY requirements.txt .
RUN python3 -m pip install -r requirements.txt

CMD ['python3']