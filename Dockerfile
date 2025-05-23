FROM debian:12-slim AS builder
ENV LANG=C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -qq && apt-get install --no-install-recommends -y python3-venv python3-setuptools python3-dev make g++ \
 && python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
ADD . /tmp/
RUN cd /tmp && pip install wheel && pip install -r /tmp/requirements.txt && python3 setup.py install

FROM debian:12-slim
ENV LANG=C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
  && apt-get install --no-install-recommends -y python3\
  && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /usr/local/bin/ /usr/local/bin/

CMD ["/usr/local/bin/esdedupe"]

