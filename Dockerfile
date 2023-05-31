FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

COPY dist dist
RUN python3 -m pip install `find dist/ -name \*.whl` 