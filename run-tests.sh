#!/usr/bin/env bash

docker -v
echo

if [[ "$(docker ps | grep --count -E 'php-phalcon-cassandra|cassandra')" != 2 ]]
then
    echo "Error: Run './install.sh' before './run-tests.sh'"
    exit 1
fi

docker exec -it php-phalcon-cassandra php -v
echo

docker exec -it php-phalcon-cassandra /home/www/vendor/codeception/codeception/codecept run unit
