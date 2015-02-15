Arbiter
=======

Arbiter is a connection proxy for PostgreSQL, intended to simplify operations of streaming
replication setups.
It maintains the status of backends and automatically routes updates to the primary.

# Configuration example

```ini
[main]
;; The address that Arbiter listens on
listen = 127.0.0.1:5433

;; Backends is a comma seperated list of backend servers.
backends = pg1:5432, pg2:5432

[health]
;; The username and password pair describe a PostgreSQL user that
;; has SELECT permissions.
username = arbiter
password = arbiter
database = arbiter
```

# TODO
- Implement all authentication mechanisms supported by PostgreSQL.
