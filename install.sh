#!/usr/bin/env bash

if [[ -z "$1" ]]
then
    version=pull
elif [[ "$1" == "pull" || "$1" == "7.1" || "$1" == "7.2" || "$1" == "7.3" ]]
then
    version="$1"
else
    echo "Usage: $0 [pull, 7.1, 7.2, 7.3]"
    echo "- pull: Pull current php-phalcon-cassandra image from repository"
    echo "- 7.1, 7.2 or 7.3: to build an image with this PHP version"
    exit 1
fi

docker --version
docker stop php-phalcon-cassandra
docker stop cassandra
docker network rm my-net
docker network create --driver=bridge --subnet=172.30.0.0/16 my-net
docker pull cassandra:3.11
docker run -d -p 0.0.0.0:9160:9160 -p 0.0.0.0:9042:9042 --network=my-net --ip=172.30.100.104 --rm --name cassandra --hostname cassandra cassandra

if [[ "$version" == "pull" ]]
then
    docker pull sachamorard/php-phalcon-cassandra
else
    docker build docker/php"$version" -t sachamorard/php-phalcon-cassandra
fi

docker run -d --name php-phalcon-cassandra --network=my-net --rm -v `pwd`:/home/www -w /home/www --add-host=cassandra:172.30.100.104 sachamorard/php-phalcon-cassandra
docker exec -it php-phalcon-cassandra composer install --prefer-source --no-interaction
sleep 20
docker exec -it cassandra cqlsh -e 'DROP KEYSPACE IF EXISTS testphalcon'
docker exec -it cassandra cqlsh -e "CREATE KEYSPACE testphalcon WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"
