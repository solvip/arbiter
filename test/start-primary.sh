#!/bin/bash

# Create the cluster if applicable
mkdir -p /var/lib/postgresql/archive
chown postgres:postgres /var/lib/postgresql/*
if ! [ -f /var/lib/postgresql/data/PG_VERSION ]; then
    gosu postgres initdb /var/lib/postgresql/data
    gosu postgres postgres --single -jE <<EOF
        CREATE DATABASE arbiter;
EOF
    gosu postgres postgres --single -jE arbiter <<EOF
        create user arbiter with encrypted password 'arbiter';
EOF
    
fi

# Configure & start
cp /arbiter/*.conf /var/lib/postgresql/data

gosu postgres postgres -D /var/lib/postgresql/data
