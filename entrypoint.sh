#!/bin/bash

if [[ -z "${TEMPLATE_CONFIG}" ]]; then
  echo "No template provided"
else
  envsubst < $TEMPLATE_CONFIG > /etc/beamium/config.yaml
fi

exec "$@"