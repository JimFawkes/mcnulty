FROM python:3.7
WORKDIR /usr/src/app

RUN mkdir mcnulty logs data
COPY requirements.txt .
RUN python3 -m pip install -r requirements.txt

COPY mcnulty/ mcnulty/

CMD ["python", "mcnulty", "--help"]