A library to control a maximized number of connections to a single PostgreSQL server. 

## Discontinued

`package:postgres` from `^3.0.0` supports connection pooling as part of the core
library. Development and advanced pooling support will be migrated to that package.

## Usage

Once you've created the `PgPool` object, you can:

- Use it as `PostgreSQLExecutionContext` (from `package:postgres`).
- Use `PgPool.run` for non-transactional batches with optional retry.
- Use `PgPool.runTx` for transactional batches with optional retry.
