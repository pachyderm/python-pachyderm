FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

COPY dist/pachyderm_sdk-0.0.1-py3-none-any.whl  .
RUN python3 -m pip install pachyderm_sdk-0.0.1-py3-none-any.whl