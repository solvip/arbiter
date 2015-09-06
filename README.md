Arbiter
=======

Arbiter is a connection proxy for PostgreSQL, intended to simplify operations of streaming
replication setups.

When you start Arbiter, it listens on the `primary` and `follower` ports defined in arbiter.cfg.
Connections to the `primary` will always be routed to the backend that has the primary role.
Connections to the `follower` will be routed to the closest backend, measured by latency.

You should configure your application to connect to the `primary` if it performs destructive operations.  Connections to the `follower` should only be used for queries.

# Failure modes
In the case where no backend is primary, arbiter will stop listening on the `primary` address.  This allows for a clean failure mode for applications.

# Configuration example

```ini
[main]
;; The addresses that Arbiter listens on
primary = 127.0.0.1:5433
follower = 127.0.0.1:5434

;; Backends is a comma seperated list of backend servers.
backends = pg1:5432, pg2:5432

[health]
;; The username and password pair describe a PostgreSQL user that
;; has SELECT permissions; used to query the status of the backends.
username = arbiter
password = arbiter
database = arbiter
```
