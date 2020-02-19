import 'dart:async';
import 'dart:io';

import 'package:executor/executor.dart';
import 'package:postgres/postgres.dart';

export 'package:postgres/postgres.dart';

/// Callback function that will be called once a connection becomes available.
typedef PgCallbackFn<R> = Future<R> Function(PostgreSQLExecutionContext c);

/// The server parameters to connect to.
class PgUrl {
  final String host;
  final int port;
  final String database;
  final String username;
  final String password;
  final bool useSecure;

  PgUrl({
    this.host,
    this.port,
    this.database,
    this.username,
    this.password,
    this.useSecure = false,
  });

  /// Parses the most common connection URL formats:
  /// - postgresql://user:password@host:port/dbname
  /// - postgresql://host:port/dbname?username=user&password=pwd
  ///
  /// Set ?sslmode=require to force secure SSL connection.
  factory PgUrl.parse(String url) {
    final uri = Uri.parse(url);
    final userInfoParts = uri.userInfo.split(':');
    final username = userInfoParts.length == 2 ? userInfoParts[0] : null;
    final password = userInfoParts.length == 2 ? userInfoParts[1] : null;
    return PgUrl(
      host: uri.host,
      port: uri.port,
      database: uri.path.substring(1),
      username: username ?? uri.queryParameters['username'],
      password: password ?? uri.queryParameters['password'],
      useSecure: uri.queryParameters['sslmode'] == 'require',
    );
  }

  /// Creates a new [PgUrl] by replacing the current values with non-null
  /// parameters.
  ///
  /// Parameters with `null` values are ignored (keeping current value).
  PgUrl replace({String database}) => PgUrl(
        host: host,
        port: port,
        database: database ?? this.database,
        username: username,
        password: password,
      );

  @override
  String toString() => Uri(
        scheme: 'postgres',
        host: host,
        port: port,
        path: database,
        queryParameters: {
          'username': username,
          'password': password,
          'sslmode': useSecure ? 'require' : 'allow',
        },
      ).toString();
}

/// The list of [PgPool] actions.
abstract class PgPoolAction {
  static final connecting = 'connecting';
  static final connectingCompleted = 'connectingCompleted';
  static final connectingFailed = 'connectingFailed';
  static final closing = 'closing';
  static final closingCompleted = 'closingCompleted';
  static final closingFailed = 'closingFailed';
  static final query = 'query';
  static final queryCompleted = 'queryCompleted';
  static final queryFailed = 'queryFailed';
}

/// Describes a pool event (with error - if there were any).
class PgPoolEvent {
  /// One of [PgPoolAction] values.
  final String action;

  /// The SQL query (if there was any).
  final String query;

  /// The SQL query's substitution values (if there was any).
  final Map<String, dynamic> substitutionValues;

  /// The elapsed time since the operation started (if applicable).
  final Duration elapsed;

  /// The error object (if there was any).
  final dynamic error;

  /// The stack trace when the error happened (if there was any).
  final StackTrace stackTrace;

  PgPoolEvent(
    this.action, {
    this.query,
    this.substitutionValues,
    this.elapsed,
    this.error,
    this.stackTrace,
  });
}

/// A snapshot of the [PgPool]'s internal state.
class PgPoolInfo {
  /// The number of connections readily available.
  final int available;

  /// The number of callbacks using an active connection.
  final int active;

  /// The number of callbacks waiting for a connection.
  final int waiting;

  PgPoolInfo({
    this.available,
    this.active,
    this.waiting,
  });
}

/// Single-server connection pool for PostgresSQL database access.
class PgPool implements PostgreSQLExecutionContext {
  final PgUrl _url;
  final int _connectTimeoutSeconds;
  final int _queryTimeoutSeconds;
  final Duration _idleTestThreshold;
  final Duration _maxAge;
  final Duration _maxInUse;
  final int _maxErrorCount;
  final int _maxSuccessCount;

  final Executor _executor;
  final _available = <_ConnectionCtx>[];
  final _events = StreamController<PgPoolEvent>.broadcast();

  /// Makes sure only one connection is opening at a time.
  Completer _openCompleter;

  PgPool(
    PgUrl url, {
    int maxActive = 2,
    int connectTimeoutSeconds = 15,
    int queryTimeoutSeconds = 180,
    Duration idleTestThreshold = const Duration(seconds: 5),
    Duration maxAge = const Duration(hours: 12),
    Duration maxInUse = const Duration(hours: 4),
    int maxErrorCount = 128,
    int maxSuccessCount = 1024 * 1024,
  })  : _executor = Executor(concurrency: maxActive),
        _url = url,
        _connectTimeoutSeconds = connectTimeoutSeconds,
        _queryTimeoutSeconds = queryTimeoutSeconds,
        _idleTestThreshold = idleTestThreshold,
        _maxAge = maxAge,
        _maxInUse = maxInUse,
        _maxErrorCount = maxErrorCount,
        _maxSuccessCount = maxSuccessCount;

  /// Get the current debug information of the pool's internal state.
  PgPoolInfo info() => PgPoolInfo(
        available: _available.length,
        active: _executor.runningCount,
        waiting: _executor.waitingCount,
      );

  /// The events that happen while the pool is working.
  Stream<PgPoolEvent> get events => _events.stream;

  /// Runs [action] outside of a transaction.
  Future<R> run<R>(
    PgCallbackFn<R> action, {
    int maxRetry = 0,
    FutureOr<R> Function() orElse,
  }) async {
    for (var i = maxRetry;; i--) {
      try {
        return await _withConnection(
          (c) => action(_PgExecutionContextWrapper(c, _events)),
        );
      } catch (e) {
        if (i == 0) {
          if (orElse != null) {
            return await orElse();
          } else {
            rethrow;
          }
        }
      }
    }
  }

  /// Runs [action] in a transaction.
  Future<R> runTx<R>(
    PgCallbackFn<R> action, {
    int maxRetry = 0,
    FutureOr<R> Function() orElse,
  }) async {
    for (var i = maxRetry;; i--) {
      try {
        return await _withConnection((conn) async {
          return await conn.transaction(
            (c) => action(_PgExecutionContextWrapper(c, _events)),
          ) as R;
        });
      } catch (e) {
        if (i == 0) {
          if (orElse != null) {
            return await orElse();
          } else {
            rethrow;
          }
        }
      }
    }
  }

  Future close() async {
    await _executor.close();
    while (_available.isNotEmpty) {
      await _close(_available.removeLast().connection);
    }
  }

  Future<R> _withConnection<R>(
      Future<R> Function(PostgreSQLConnection c) body) {
    return _executor.scheduleTask(() async {
      final sw = Stopwatch()..start();
      try {
        return await _useOrCreate(body);
      } finally {
        sw.stop();
        if (sw.elapsed.inSeconds >= 5) {
          print('slow... ${sw.elapsed}');
        }
      }
    });
  }

  Future<R> _useOrCreate<R>(
      Future<R> Function(PostgreSQLConnection c) body) async {
    _ConnectionCtx ctx;
    while (_available.isNotEmpty) {
      final entry = _available.removeLast();
      if (await _testConnection(entry)) {
        ctx = entry;
        break;
      } else {
        await _close(entry.connection);
      }
    }
    for (var i = 3; i > 0; i--) {
      _events.add(PgPoolEvent(PgPoolAction.connecting));
      final sw = Stopwatch()..start();
      try {
        ctx ??= await _open();
        _events.add(PgPoolEvent(
          PgPoolAction.connectingCompleted,
          elapsed: sw.elapsed,
        ));
      } catch (e, st) {
        _events.add(PgPoolEvent(
          PgPoolAction.connectingFailed,
          elapsed: sw.elapsed,
          error: e,
          stackTrace: st,
        ));
        if (i == 1) {
          rethrow;
        }
      }
    }
    final sw = Stopwatch()..start();
    try {
      final r = await body(ctx.connection);
      ctx.lastReturned = DateTime.now();
      ctx.successCount++;
      _available.add(ctx);
      return r;
    } catch (e) {
      if (e is PostgreSQLException ||
          e is IOException ||
          e is TimeoutException) {
        await _close(ctx.connection);
      } else {
        ctx.lastReturned = DateTime.now();
        ctx.errorCount++;
        _available.add(ctx);
      }
      rethrow;
    } finally {
      sw.stop();
      ctx.elapsed += sw.elapsed;
    }
  }

  Future<_ConnectionCtx> _open() async {
    while (_openCompleter != null) {
      await _openCompleter.future;
    }
    _openCompleter = Completer();
    try {
      final url = _url;
      final c = PostgreSQLConnection(
        url.host,
        url.port,
        url.database,
        username: url.username,
        password: url.password,
        useSSL: url.useSecure,
        timeoutInSeconds: _connectTimeoutSeconds,
        queryTimeoutInSeconds: _queryTimeoutSeconds,
      );
      await c.open();
      return _ConnectionCtx(c);
    } finally {
      final c = _openCompleter;
      _openCompleter = null;
      c.complete();
    }
  }

  Future<bool> _testConnection(_ConnectionCtx ctx) async {
    final now = DateTime.now();
    final totalAge = now.difference(ctx.created).abs();
    final shouldClose = (totalAge >= _maxAge) ||
        (ctx.elapsed >= _maxInUse) ||
        (ctx.errorCount >= _maxErrorCount) ||
        (ctx.successCount >= _maxSuccessCount);
    if (shouldClose) {
      return false;
    }
    final idleAge = now.difference(ctx.lastReturned).abs();
    if (idleAge < _idleTestThreshold) {
      return true;
    }
    try {
      await ctx.connection.query('SELECT 1;', timeoutInSeconds: 2);
      return true;
    } catch (_) {}
    return false;
  }

  Future _close(PostgreSQLConnection c) async {
    final sw = Stopwatch()..start();
    try {
      if (!c.isClosed) {
        _events.add(PgPoolEvent(PgPoolAction.closing));
        await c.close();
        _events.add(
            PgPoolEvent(PgPoolAction.closingCompleted, elapsed: sw.elapsed));
      }
    } catch (e, st) {
      _events.add(PgPoolEvent(
        PgPoolAction.closingFailed,
        elapsed: sw.elapsed,
        error: e,
        stackTrace: st,
      ));
    }
  }

  @override
  int get queueSize => null;

  @override
  Future<PostgreSQLResult> query(String fmtString,
      {Map<String, dynamic> substitutionValues,
      bool allowReuse = true,
      int timeoutInSeconds}) {
    return run(
      (c) => c.query(
        fmtString,
        substitutionValues: substitutionValues,
        allowReuse: allowReuse,
        timeoutInSeconds: timeoutInSeconds,
      ),
    );
  }

  @override
  Future<int> execute(String fmtString,
      {Map<String, dynamic> substitutionValues, int timeoutInSeconds}) {
    return run(
      (c) => c.execute(
        fmtString,
        substitutionValues: substitutionValues,
        timeoutInSeconds: timeoutInSeconds,
      ),
    );
  }

  @override
  void cancelTransaction({String reason}) {
    // no-op
  }

  @override
  Future<List<Map<String, Map<String, dynamic>>>> mappedResultsQuery(
      String fmtString,
      {Map<String, dynamic> substitutionValues,
      bool allowReuse = true,
      int timeoutInSeconds}) {
    return run(
      (c) => c.mappedResultsQuery(
        fmtString,
        substitutionValues: substitutionValues,
        allowReuse: allowReuse,
        timeoutInSeconds: timeoutInSeconds,
      ),
    );
  }
}

class _ConnectionCtx {
  final PostgreSQLConnection connection;
  final DateTime created;
  DateTime lastReturned;
  int successCount = 0;
  int errorCount = 0;
  Duration elapsed = Duration.zero;

  _ConnectionCtx(this.connection) : created = DateTime.now();
}

class _PgExecutionContextWrapper implements PostgreSQLExecutionContext {
  final PostgreSQLExecutionContext _delegate;
  final Sink<PgPoolEvent> _eventSink;

  _PgExecutionContextWrapper(this._delegate, this._eventSink);

  Future<R> _run<R>(
    Future<R> Function() body,
    String query,
    Map<String, dynamic> substitutionValues,
  ) async {
    final sw = Stopwatch()..start();
    try {
      _eventSink.add(PgPoolEvent(
        PgPoolAction.query,
        query: query,
        substitutionValues: substitutionValues,
      ));
      final r = await body();
      _eventSink.add(PgPoolEvent(
        PgPoolAction.queryCompleted,
        query: query,
        substitutionValues: substitutionValues,
        elapsed: sw.elapsed,
      ));
      return r;
    } catch (e, st) {
      _eventSink.add(PgPoolEvent(
        PgPoolAction.queryFailed,
        query: query,
        substitutionValues: substitutionValues,
        elapsed: sw.elapsed,
        error: e,
        stackTrace: st,
      ));
      rethrow;
    } finally {
      sw.stop();
    }
  }

  @override
  void cancelTransaction({String reason}) {
    _delegate.cancelTransaction(reason: reason);
  }

  @override
  Future<int> execute(String fmtString,
      {Map<String, dynamic> substitutionValues, int timeoutInSeconds}) {
    return _run(
      () => _delegate.execute(
        fmtString,
        substitutionValues: substitutionValues,
        timeoutInSeconds: timeoutInSeconds,
      ),
      fmtString,
      substitutionValues,
    );
  }

  @override
  Future<PostgreSQLResult> query(String fmtString,
      {Map<String, dynamic> substitutionValues,
      bool allowReuse = true,
      int timeoutInSeconds}) {
    return _run(
      () => _delegate.query(
        fmtString,
        substitutionValues: substitutionValues,
        allowReuse: allowReuse,
        timeoutInSeconds: timeoutInSeconds,
      ),
      fmtString,
      substitutionValues,
    );
  }

  @override
  Future<List<Map<String, Map<String, dynamic>>>> mappedResultsQuery(
      String fmtString,
      {Map<String, dynamic> substitutionValues,
      bool allowReuse = true,
      int timeoutInSeconds}) {
    return _run(
      () => _delegate.mappedResultsQuery(
        fmtString,
        substitutionValues: substitutionValues,
        allowReuse: allowReuse,
        timeoutInSeconds: timeoutInSeconds,
      ),
      fmtString,
      substitutionValues,
    );
  }

  @override
  int get queueSize => _delegate.queueSize;
}
