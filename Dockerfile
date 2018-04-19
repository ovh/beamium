FROM debian:jessie

ENV DEBIAN_FRONTEND=noninteractive
ARG METRICS_APT_URL=http://last.public.ovh.metrics.snap.mirrors.ovh.net

RUN    apt-get update \
    && apt-get install -y --no-install-recommends \
         gnupg \
    && echo "deb $METRICS_APT_URL/debian jessie main" >> /etc/apt/sources.list.d/metrics.list \
    && apt-key adv \
         --keyserver $METRICS_APT_URL/pub.key \
         --recv-keys A7F0D217C80D5BB8 \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        libssl-dev \
        beamium \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

CMD ["beamium"]
