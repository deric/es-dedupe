FROM debian:9-slim as builder
ENV LANG C.UTF-8
RUN apt-get update && apt-get install --no-install-recommends -y python3-pip python3-setuptools python3-dev make gcc\
 && apt-get clean && rm -rf /var/lib/apt/lists/*
ADD requirements.txt /tmp/
RUN pip3 install wheel && pip3 install -r /tmp/requirements.txt

FROM debian:9-slim
ENV LANG C.UTF-8

RUN apt-get update \
  && apt-get install --no-install-recommends -y python3\
  && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/lib/python3.5/ /usr/local/lib/python3.5/
#COPY --from=builder /usr/local/lib/python3.5/site-packages/ /usr/local/lib/python3.5/site-packages/

RUN mkdir /app
ADD dedupe.py /app
ADD entrypoint.sh /app
WORKDIR /app
ENTRYPOINT /app/entrypoint.sh

