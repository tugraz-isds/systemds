FROM openjdk:8-alpine

RUN apk add --no-cache --virtual .build-deps g++ python3-dev libffi-dev openssl-dev && \
	apk add --no-cache python3 && \
    pip3 install --upgrade pip setuptools

RUN pip3 install systemds