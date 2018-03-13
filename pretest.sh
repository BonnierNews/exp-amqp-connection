#!/bin/bash

DONE='\033[0;32mdone\033[0m'

function rabbitmq_ready {
  docker-compose exec rabbitmq rabbitmqctl node_health_check 2>&1 >> /dev/null
}

if test -z "$(docker ps | grep rabbitmq)"; then
    docker-compose up -d
    # wait for rabbit ready
    echo -n "RabbitMQ starting"
    until rabbitmq_ready; do echo -n "."; done
    echo -e " $DONE"
fi
