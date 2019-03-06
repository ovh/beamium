FROM debian:stretch

ENV DEBIAN_FRONTEND=noninteractive
ARG METRICS_APT_URL=http://last.public.ovh.metrics.snap.mirrors.ovh.net

RUN apt-get update \
        && apt-get install -y apt-transport-https curl gnupg gettext-base ca-certificates \
        && echo "deb $METRICS_APT_URL/debian stretch main" >> /etc/apt/sources.list.d/beamium.list \
        && curl https://last-public-ovh-metrics.snap.mirrors.ovh.net/pub.key | apt-key add - \
        && apt-get update \
        && apt-get install -y beamium \
        && rm -rf /var/lib/apt/lists/*

ADD entrypoint.sh /

ENTRYPOINT ["/entrypoint.sh"]
CMD ["beamium"]
