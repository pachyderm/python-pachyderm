FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

COPY dist/python_pachyderm-7.4.0-py3-none-any.whl .
RUN python3 -m pip install python_pachyderm-7.4.0-py3-none-any.whl