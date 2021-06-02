FROM python:3.6-slim
LABEL maintainer="jdoliner@pachyderm.io"

RUN pip3 install grpcio==1.38.0 grpcio-tools==1.38.0

ADD run /bin
ENTRYPOINT ["/bin/run"]
WORKDIR /work
