## 2.1.0-null-safety.2

- Fixed public API changes from upstream `package:postgres`.

## 2.1.0-null-safety.1

- Migration to null-safety.
- **BREAKING CHANGE**: `PgEndpoint` required fields: `host` and `database`.

## 2.0.1

- More efficient way to close the pool.

## 2.0.0

**BREAKING CHANGES**: refactor to match `package:cockroachdb_pool`'s requirements.

- Renamed or refactored:
  - `PgCallbackFn` -> `PgSessionFn`
  - `PgUrl` -> `PgEndpoint`
    - `useSecure` -> `requireSsl`
  - `PgPool`
    - Constructor parameters moved into `PgPoolSettings`, which allows
      tuning them while the pool is running.
    - `info()` -> `status()`
  - `PgPoolInfo` -> `PgPoolStatus`
    - `available` removed, use `connections` instead.
    - `active` -> `activeSessionCount`
    - `waiting` -> `pendingSessionCount`
- Using `package:retry`.

## 1.1.0

- Only one connection is opened at a time.
- Randomized connection selection when multiple one is available.
  (Provides better distribution of queries, less overuse of single connection thread.)
- Generated `==` and `hashCode` for `PgUrl`.
- Removed leftover debug `print`.

## 1.0.0

- Initial public release.
