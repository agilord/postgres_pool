## 2.1.4

- Fixed API changes from `package:postgres` 2.5.0.

## 2.1.3

- Added `PgEndpoint.isUnixSocket`. ([9](https://github.com/agilord/postgres_pool/pull/9) by [davidmartos96](https://github.com/davidmartos96))

## 2.1.2

- Added `PgPoolSettings.timeZone`. ([#5](https://github.com/agilord/postgres_pool/pull/5) by [eduardotd](https://github.com/eduardotd))

## 2.1.1

- Removed bang operators from optional parameters. ([#3](https://github.com/agilord/postgres_pool/pull/3) by [jimmyff](https://github.com/jimmyff))

## 2.1.0

- Final null-safe release.

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
