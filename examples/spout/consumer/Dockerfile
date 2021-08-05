FROM python:3.6-slim-stretch
COPY requirements.txt /app/requirements.txt
RUN pip3 install -r /app/requirements.txt
COPY poll_consumer.py /app/poll_consumer.py