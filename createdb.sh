#!/usr/bin/env bash
docker exec -it cassandra cqlsh -e 'DROP KEYSPACE IF EXISTS testphalcon'
docker exec -it cassandra cqlsh -e "CREATE KEYSPACE testphalcon WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"