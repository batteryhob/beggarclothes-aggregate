FROM ubuntu:18.04

RUN apt-get update && apt-get install -y \
	wget \
	xvfb \
	unzip \
	python3-pip \
	python3-dev \
	python3-setuptools

RUN mkdir -p /app

WORKDIR /app

COPY . /app

RUN pip3 install configparser
RUN pip3 install pymysql
RUN pip3 install boto3
RUN pip3 install pytz

ENV LC_ALL=C.UTF-8
ENV PYTHONUNBUFFERED 0

EXPOSE 4001

ENTRYPOINT ["python3", "/app/aggregate.py"]