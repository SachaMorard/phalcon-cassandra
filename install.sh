#!/usr/bin/env bash

docker --version
docker stop php-phalcon-cassandra
docker stop cassandra
docker network rm my-net
docker network create --driver=bridge --subnet=172.30.0.0/16 my-net
docker pull cassandra:3.10
docker run -d -p 0.0.0.0:9160:9160 -p 0.0.0.0:9042:9042 --network=my-net --ip=172.30.100.104 --rm --name cassandra --hostname cassandra cassandra
#docker build docker/php7.1 -t sachamorard/php-phalcon-cassandra
docker pull sachamorard/php-phalcon-cassandra
docker run -d --name php-phalcon-cassandra --network=my-net --rm -v `pwd`:/home/www -w /home/www --add-host=cassandra:172.30.100.104 sachamorard/php-phalcon-cassandra
docker exec -it php-phalcon-cassandra composer install --prefer-source --no-interaction
sleep 20
docker exec -it cassandra cqlsh -e 'DROP KEYSPACE IF EXISTS testphalcon'
docker exec -it cassandra cqlsh -e "CREATE KEYSPACE testphalcon WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"