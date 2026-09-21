#!/bin/sh
set -eu

rabbitmqadmin definitions import --file /etc/rabbitmq/definitions.json
