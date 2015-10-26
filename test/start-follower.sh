#!/bin/bash

chown postgres:postgres /var/lib/postgresql/data
chmod 0700 /var/lib/postgresql/data

# Wait for the primary to boot
while ! nc -z $PRIMARY_1_PORT_5432_TCP_ADDR $PRIMARY_1_PORT_5432_TCP_PORT; do
    sleep 0.5s
done

# Copy the database
rm -rf /var/lib/postgresql/data/*
gosu postgres pg_basebackup -D /var/lib/postgresql/data -w -R --xlog-method=stream --dbname="host=$PRIMARY_1_PORT_5432_TCP_ADDR user=postgres"

# Start it
gosu postgres postgres -D /var/lib/postgresql/data
