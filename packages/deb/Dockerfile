FROM ubuntu:latest

RUN apt-get update
RUN apt-get install -y gdebi-core
ADD target/packages/beamium*.deb var/beamium.deb
RUN gdebi -n /var/beamium.deb

# Various tests and checks
RUN ls -lh /usr/bin/beamium
RUN ls -lh /etc/beamium/config.yaml
RUN ls -lh /opt/beamium
RUN ls -lh /var/log/beamium
RUN ls -lh /etc/logrotate.d/beamium
RUN ls -lh /etc/init.d/beamium


CMD ["/bin/bash"]
