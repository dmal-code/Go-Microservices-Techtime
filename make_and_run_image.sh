#!/bin/bash

#go get -u github.com/google/pprof

docker build --network="host" -t docker_go .
docker-compose stop go_slave1 go_slave2 go_slave3 go_slave4 go_slave5
docker-compose rm --force go_slave1 go_slave2 go_slave3 go_slave4 go_slave5
docker-compose up -d go_slave1 go_slave2 go_slave3 go_slave4 go_slave5
echo "Waiting some time in order to let the slaves register their queues"
sleep 10
docker-compose up go
docker-compose rm --force go
docker-compose stop go_slave1 go_slave2 go_slave3 go_slave4 go_slave5
docker-compose rm --force go_slave1 go_slave2 go_slave3 go_slave4 go_slave5
#docker run -it --network="bridge" -p 0.0.0.0:899:80 --rm --name go_app_test docker_go