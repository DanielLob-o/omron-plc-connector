FROM python:3.8-slim-buster
ARG GIT_TOKEN_USER
ARG GIT_TOKEN

WORKDIR /code
COPY . .
RUN apt-get update && apt-get install -y iputils-ping
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt
 CMD [ "python", "main.py" ]