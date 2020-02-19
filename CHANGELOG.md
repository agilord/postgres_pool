## 1.1.0

- Only one connection is opened at a time.
- Randomized connection selection when multiple one is available.
  (Provides better distribution of queries, less overuse of single connection thread.)
- Generated `==` and `hashCode` for `PgUrl`.
- Removed leftover debug `print`.

## 1.0.0

- Initial public release.
