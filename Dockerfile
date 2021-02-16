FROM debian:10-slim as builder
ENV LANG C.UTF-8
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && apt-get install --no-install-recommends -y python3-pip python3-setuptools python3-dev make gcc
ADD . /tmp/
RUN cd /tmp && pip3 install wheel && pip3 install -r /tmp/requirements.txt && python3 setup.py install

FROM debian:10-slim
ENV LANG C.UTF-8
ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update \
  && apt-get install --no-install-recommends -y python3\
  && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/lib/python3.7/ /usr/local/lib/python3.7/
COPY --from=builder /usr/lib/python3/dist-packages/ /usr/lib/python3/dist-packages/
COPY --from=builder /usr/local/bin/ /usr/local/bin/

RUN useradd debian
USER debian
CMD /usr/local/bin/esdedupe

