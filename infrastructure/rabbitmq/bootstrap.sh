#!/bin/sh
set -eu

attempt=1
while ! rabbitmqadmin definitions import --file /etc/rabbitmq/definitions.json; do
  if [ "$attempt" -ge 30 ]; then
    exit 1
  fi

  attempt=$((attempt + 1))
  sleep 1
done
