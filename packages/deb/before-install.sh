#!/bin/bash
useradd beamium
mkdir /opt/beamium
chown -R beamium:beamium /opt/beamium
mkdir /var/log/beamium
chown -R beamium:beamium /var/log/beamium
