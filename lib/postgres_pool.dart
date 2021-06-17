import 'dart:async';
import 'dart:io';
import 'dart:math';

import 'package:executor/executor.dart';
import 'package:postgres/postgres.dart';
import 'package:retry/retry.dart';

export 'package:postgres/postgres.dart';

/// A session is a continuous use of a single connection (inside or outside of a
/// transaction).
///
/// This callback function will be called once a connection becomes available.
typedef PgSessionFn<R> = Future<R> Function(PostgreSQLExecutionContext c);

/// The PostgreSQL server endpoint and its configuration to use when opening a
/// new connection.
class PgEndpoint {
  final String host;
  final int port;
  final String database;
  final String? username;
  final String? password;
  final bool requireSsl;

  PgEndpoint({
    required this.host,
    this.port = 5432,
    required this.database,
    this.username,
    this.password,
    this.requireSsl = false,
  });

  /// Parses the most common connection URL formats:
  /// - postgresql://user:password@host:port/dbname
  /// - postgresql://host:port/dbname?username=user&password=pwd
  ///
  /// Set ?sslmode=require to force secure SSL connection.
  factory PgEndpoint.parse(url) {
    final uri = url is Uri ? url : Uri.parse(url as String);
    final userInfoParts = uri.userInfo.split(':');
    final username = userInfoParts.length == 2 ? userInfoParts[0] : null;
    final password = userInfoParts.length == 2 ? userInfoParts[1] : null;
    return PgEndpoint(
      host: uri.host,
      port: uri.port,
      database: uri.path.substring(1),
      username: username ?? uri.queryParameters['username'],
      password: password ?? uri.queryParameters['password'],
      requireSsl: uri.queryParameters['sslmode'] == 'require',
    );
  }

  /// Creates a new [PgEndpoint] by replacing the current values with non-null
  /// parameters.
  ///
  /// Parameters with `null` values are ignored (keeping current value).
  PgEndpoint replace({
    String? host,
    int? port,
    String? database,
    String? username,
    String? password,
    bool? requireSsl,
  }) {
    return PgEndpoint(
      host: host ?? this.host,
      port: port ?? this.port,
      database: database ?? this.database,
      username: username ?? this.username,
      password: password ?? this.password,
      requireSsl: requireSsl ?? this.requireSsl,
    );
  }

  @override
  String toString() => Uri(
        scheme: 'postgres',
        host: host,
        port: port,
        path: database,
        queryParameters: {
          'username': username,
          'password': password,
          'sslmode': requireSsl ? 'require' : 'allow',
        },
      ).toString();

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is PgEndpoint &&
          runtimeType == other.runtimeType &&
          host == other.host &&
          port == other.port &&
          database == other.database &&
          username == other.username &&
          password == other.password &&
          requireSsl == other.requireSsl;

  @override
  int get hashCode =>
      host.hashCode ^
      port.hashCode ^
      database.hashCode ^
      username.hashCode ^
      password.hashCode ^
      requireSsl.hashCode;
}

/// The list of [PgPool] actions.
abstract class PgPoolAction {
  static final connecting = 'connecting';
  static final connectingCompleted = 'connecting-completed';
  static final connectingFailed = 'connecting-failed';
  static final closing = 'closing';
  static final closingCompleted = 'closing-completed';
  static final closingFailed = 'closing-failed';
  static final query = 'query';
  static final queryCompleted = 'query-completed';
  static final queryFailed = 'query-failed';
}

/// Describes a pool event (with error - if there were any).
class PgPoolEvent {
  /// The id of the connection
  final int connectionId;

  /// The identifier of the session (e.g. activity type)
  final String? sessionId;

  /// The unique identifier of a request (used for correlating log entries).
  final String? traceId;

  /// One of [PgPoolAction] values.
  final String action;

  /// The SQL query (if there was any).
  final String? query;

  /// The SQL query's substitution values (if there was any).
  final Map<String, dynamic>? substitutionValues;

  /// The elapsed time since the operation started (if applicable).
  final Duration? elapsed;

  /// The error object (if there was any).
  final dynamic error;

  /// The stack trace when the error happened (if there was any).
  final StackTrace? stackTrace;

  PgPoolEvent({
    required this.connectionId,
    this.sessionId,
    this.traceId,
    required this.action,
    this.query,
    this.substitutionValues,
    this.elapsed,
    this.error,
    this.stackTrace,
  });

  PgPoolEvent.fromPgPoolEvent(PgPoolEvent other)
      : connectionId = other.connectionId,
        sessionId = other.sessionId,
        traceId = other.traceId,
        action = other.action,
        query = other.query,
        substitutionValues = other.substitutionValues,
        elapsed = other.elapsed,
        error = other.error,
        stackTrace = other.stackTrace;
}

/// A snapshot of the connection's internal state.
class PgConnectionStatus {
  /// The numerical id of the connection.
  final int connectionId;

  /// If the connection is open and not in use.
  final bool isIdle;

  /// The time when the connection was opened.
  final DateTime opened;

  PgConnectionStatus({
    required this.connectionId,
    required this.isIdle,
    required this.opened,
  });
}

/// A snapshot of the [PgPool]'s internal state.
class PgPoolStatus {
  /// The status of the connections.
  final List<PgConnectionStatus> connections;

  /// The number of sessions using an active connection.
  final int activeSessionCount;

  /// The number of sessions waiting for a connection to become available.
  final int pendingSessionCount;

  PgPoolStatus({
    required this.connections,
    required this.activeSessionCount,
    required this.pendingSessionCount,
  });

  PgPoolStatus.fromPgPoolStatus(PgPoolStatus other)
      : connections = other.connections,
        activeSessionCount = other.activeSessionCount,
        pendingSessionCount = other.pendingSessionCount;
}

/// The settings of the [PgPool].
class PgPoolSettings {
  /// The maximum number of concurrent sessions.
  int concurrency = 1;

  /// The timeout after the connection attempt is assumed to be failing.
  /// Fractional seconds will be omitted.
  /// Value is applied only on new connections.
  Duration connectTimeout = Duration(seconds: 15);

  /// The timeout after a query is assumed to be failing.
  /// Fractional seconds will be omitted.
  /// Value is applied only on new connections.
  Duration queryTimeout = Duration(minutes: 5);

  /// If a connection is idle for longer than this threshold, it will be tested
  /// with a simple SQL query before allocating it to a session.
  Duration idleTestThreshold = Duration(minutes: 1);

  /// The maximum duration a connection is kept open.
  /// New sessions won't be scheduled after this limit is reached.
  Duration maxConnectionAge = Duration(hours: 12);

  /// The maximum duration a connection is used by sessions.
  /// New sessions won't be scheduled after this limit is reached.
  Duration maxSessionUse = Duration(hours: 8);

  /// The maximum number of error events to be collected on a connection.
  /// New sessions won't be scheduled after this limit is reached.
  int maxErrorCount = 128;

  /// The maximum number of queries to be run on a connection.
  /// New sessions won't be scheduled after this limit is reached.
  int maxQueryCount = 1024 * 1024;

  /// The default retry options for `run` / `runTx` operations.
  RetryOptions retryOptions = RetryOptions(
    maxAttempts: 1,
    delayFactor: Duration(milliseconds: 5),
    maxDelay: Duration(seconds: 1),
    randomizationFactor: 0.1,
  );

  void applyFrom(PgPoolSettings other) {
    concurrency = other.concurrency;
    connectTimeout = other.connectTimeout;
    queryTimeout = other.queryTimeout;
    idleTestThreshold = other.idleTestThreshold;
    maxConnectionAge = other.maxConnectionAge;
    maxSessionUse = other.maxSessionUse;
    maxErrorCount = other.maxErrorCount;
    maxQueryCount = other.maxQueryCount;
    retryOptions = other.retryOptions;
  }
}

/// Single-server connection pool for PostgresSQL database access.
class PgPool implements PostgreSQLExecutionContext {
  final PgEndpoint _url;

  final Executor _executor;
  final _random = Random();
  final _connections = <_ConnectionCtx>[];
  final _events = StreamController<PgPoolEvent>.broadcast();
  int _nextConnectionId = 1;

  /// Makes sure only one connection is opening at a time.
  Completer? _openCompleter;
  PgPoolSettings settings;

  PgPool(PgEndpoint url, {PgPoolSettings? settings})
      : settings = settings ?? PgPoolSettings(),
        _executor = Executor(),
        _url = url;

  /// Get the current debug information of the pool's internal state.
  PgPoolStatus status() => PgPoolStatus(
        connections: _connections.map((c) => c.status()).toList(),
        activeSessionCount: _executor.runningCount,
        pendingSessionCount: _executor.waitingCount,
      );

  /// The events that happen while the pool is working.
  Stream<PgPoolEvent> get events => _events.stream;

  /// Runs [fn] outside of a transaction.
  Future<R> run<R>(
    PgSessionFn<R> fn, {
    RetryOptions? retryOptions,
    FutureOr<R> Function()? orElse,
    FutureOr<bool> Function(Exception)? retryIf,
    String? sessionId,
    String? traceId,
  }) async {
    retryOptions ??= settings.retryOptions;
    try {
      return await retryOptions.retry(
        () async {
          return await _withConnection(
            (c) => fn(_PgExecutionContextWrapper(
              c.connectionId,
              c.connection,
              sessionId,
              traceId,
              _events,
            )),
          );
        },
        retryIf: (e) async =>
            e is! PostgreSQLException &&
            e is! IOException &&
            (retryIf == null || await retryIf(e)),
      );
    } catch (e) {
      if (orElse != null) {
        return await orElse();
      }
      rethrow;
    }
  }

  /// Runs [fn] in a transaction.
  Future<R> runTx<R>(
    PgSessionFn<R> fn, {
    RetryOptions? retryOptions,
    FutureOr<R> Function()? orElse,
    FutureOr<bool> Function(Exception)? retryIf,
    String? sessionId,
    String? traceId,
  }) async {
    retryOptions ??= settings.retryOptions;
    try {
      return await retryOptions.retry(
        () async {
          return await _withConnection((c) async {
            return await c.connection.transaction(
              (conn) => fn(_PgExecutionContextWrapper(
                c.connectionId,
                conn,
                sessionId,
                traceId,
                _events,
              )),
            ) as R;
          });
        },
        retryIf: (e) async =>
            e is! PostgreSQLException &&
            e is! IOException &&
            (retryIf == null || await retryIf(e)),
      );
    } catch (e) {
      if (orElse != null) {
        return await orElse();
      }
      rethrow;
    }
  }

  Future close() async {
    await _executor.close();
    while (_connections.isNotEmpty) {
      await Future.wait(_connections.map(_close));
    }
    await _events.close();
  }

  Future<R> _withConnection<R>(Future<R> Function(_ConnectionCtx c) body) {
    _executor.concurrency = settings.concurrency;
    return _executor.scheduleTask(() async {
      return await _useOrCreate(body);
    });
  }

  _ConnectionCtx? _lockIdle() {
    final list = _connections.where((c) => c.isIdle).toList();
    if (list.isEmpty) return null;
    final entry =
        list.length == 1 ? list.single : list[_random.nextInt(list.length)];
    entry.isIdle = false;
    return entry;
  }

  Future<_ConnectionCtx?> _tryAcquireAvailable() async {
    for (var ctx = _lockIdle(); ctx != null; ctx = _lockIdle()) {
      if (await _testConnection(ctx)) {
        return ctx;
      } else {
        await _close(ctx);
      }
    }
    return null;
  }

  Future<R> _useOrCreate<R>(Future<R> Function(_ConnectionCtx c) body) async {
    final ctx = await _tryAcquireAvailable() ?? await _open();
    final sw = Stopwatch()..start();
    try {
      final r = await body(ctx);
      ctx.lastReturned = DateTime.now();
      ctx.elapsed += sw.elapsed;
      ctx.queryCount++;
      ctx.isIdle = true;
      return r;
    } on PostgreSQLException catch (_) {
      await _close(ctx);
      rethrow;
    } on IOException catch (_) {
      await _close(ctx);
      rethrow;
    } on TimeoutException catch (_) {
      await _close(ctx);
      rethrow;
    } catch (e) {
      ctx.lastReturned = DateTime.now();
      ctx.elapsed += sw.elapsed;
      ctx.errorCount++;
      ctx.queryCount++;
      ctx.isIdle = true;
      rethrow;
    }
  }

  Future<_ConnectionCtx> _open() async {
    while (_openCompleter != null) {
      await _openCompleter!.future;
    }
    _openCompleter = Completer();
    final connectionId = _nextConnectionId++;
    _events.add(PgPoolEvent(
      connectionId: connectionId,
      action: PgPoolAction.connecting,
    ));
    try {
      for (var i = 3; i > 0; i--) {
        final sw = Stopwatch()..start();
        try {
          final c = PostgreSQLConnection(
            _url.host,
            _url.port,
            _url.database,
            username: _url.username,
            password: _url.password,
            useSSL: _url.requireSsl,
            timeoutInSeconds: settings.connectTimeout.inSeconds,
            queryTimeoutInSeconds: settings.queryTimeout.inSeconds,
          );
          await c.open();
          final ctx = _ConnectionCtx(connectionId, c);
          _connections.add(ctx);

          _events.add(PgPoolEvent(
            connectionId: connectionId,
            action: PgPoolAction.connectingCompleted,
            elapsed: sw.elapsed,
          ));

          return ctx;
        } catch (e, st) {
          if (i == 1) {
            _events.add(PgPoolEvent(
              connectionId: connectionId,
              action: PgPoolAction.connectingFailed,
              elapsed: sw.elapsed,
              error: e,
              stackTrace: st,
            ));
            rethrow;
          }
        }
      }
      throw StateError('Should not reach this code.');
    } finally {
      final c = _openCompleter!;
      _openCompleter = null;
      c.complete();
    }
  }

  Future<bool> _testConnection(_ConnectionCtx ctx) async {
    final now = DateTime.now();
    final totalAge = now.difference(ctx.opened).abs();
    final shouldClose = (totalAge >= settings.maxConnectionAge) ||
        (ctx.elapsed >= settings.maxSessionUse) ||
        (ctx.errorCount >= settings.maxErrorCount) ||
        (ctx.queryCount >= settings.maxQueryCount);
    if (shouldClose) {
      return false;
    }
    final idleAge = now.difference(ctx.lastReturned).abs();
    if (idleAge < settings.idleTestThreshold) {
      return true;
    }
    try {
      await ctx.connection.query('SELECT 1;', timeoutInSeconds: 2);
      return true;
    } catch (_) {}
    return false;
  }

  Future _close(_ConnectionCtx ctx) async {
    ctx.isIdle = false;
    if (ctx.closingCompleter != null) {
      await ctx.closingCompleter!.future;
      return;
    }
    ctx.closingCompleter = Completer();

    final sw = Stopwatch()..start();
    try {
      if (!ctx.connection.isClosed) {
        _events.add(PgPoolEvent(
          connectionId: ctx.connectionId,
          action: PgPoolAction.closing,
        ));
        await ctx.connection.close();
        _events.add(PgPoolEvent(
          connectionId: ctx.connectionId,
          action: PgPoolAction.closingCompleted,
          elapsed: sw.elapsed,
        ));
      }
    } catch (e, st) {
      _events.add(PgPoolEvent(
        connectionId: ctx.connectionId,
        action: PgPoolAction.closingFailed,
        elapsed: sw.elapsed,
        error: e,
        stackTrace: st,
      ));
    } finally {
      _connections.remove(ctx);
    }
  }

  @override
  int get queueSize =>
      _connections.fold<int>(0, (a, b) => a + b.connection.queueSize);

  @override
  Future<PostgreSQLResult> query(
    String fmtString, {
    Map<String, dynamic>? substitutionValues,
    bool? allowReuse = true,
    int? timeoutInSeconds,
    String? sessionId,
    String? traceId,
  }) {
    return run(
      (c) => c.query(
        fmtString,
        substitutionValues: substitutionValues,
        allowReuse: allowReuse,
        timeoutInSeconds: timeoutInSeconds,
      ),
      sessionId: sessionId,
      traceId: traceId,
    );
  }

  @override
  Future<int> execute(
    String fmtString, {
    Map<String, dynamic>? substitutionValues,
    int? timeoutInSeconds,
    String? sessionId,
    String? traceId,
  }) {
    return run(
      (c) => c.execute(
        fmtString,
        substitutionValues: substitutionValues,
        timeoutInSeconds: timeoutInSeconds,
      ),
      sessionId: sessionId,
      traceId: traceId,
    );
  }

  @override
  void cancelTransaction({String? reason}) {
    // no-op
  }

  @override
  Future<List<Map<String, Map<String, dynamic>>>> mappedResultsQuery(
    String fmtString, {
    Map<String, dynamic>? substitutionValues,
    bool? allowReuse = true,
    int? timeoutInSeconds,
    String? sessionId,
    String? traceId,
  }) {
    return run(
      (c) => c.mappedResultsQuery(
        fmtString,
        substitutionValues: substitutionValues,
        allowReuse: allowReuse,
        timeoutInSeconds: timeoutInSeconds,
      ),
      sessionId: sessionId,
      traceId: traceId,
    );
  }
}

class _ConnectionCtx {
  final int connectionId;
  final PostgreSQLConnection connection;
  final DateTime opened;
  late DateTime lastReturned;
  int queryCount = 0;
  int errorCount = 0;
  Duration elapsed = Duration.zero;
  bool isIdle = false;
  Completer? closingCompleter;

  _ConnectionCtx(this.connectionId, this.connection) : opened = DateTime.now();

  PgConnectionStatus status() {
    return PgConnectionStatus(
      connectionId: connectionId,
      isIdle: isIdle,
      opened: opened,
    );
  }
}

class _PgExecutionContextWrapper implements PostgreSQLExecutionContext {
  final int connectionId;
  final PostgreSQLExecutionContext _delegate;
  final String? sessionId;
  final String? traceId;
  final Sink<PgPoolEvent> _eventSink;

  _PgExecutionContextWrapper(
    this.connectionId,
    this._delegate,
    this.sessionId,
    this.traceId,
    this._eventSink,
  );

  Future<R> _run<R>(
    Future<R> Function() body,
    String query,
    Map<String, dynamic>? substitutionValues,
  ) async {
    final sw = Stopwatch()..start();
    try {
      _eventSink.add(PgPoolEvent(
        connectionId: connectionId,
        sessionId: sessionId,
        traceId: traceId,
        action: PgPoolAction.query,
        query: query,
        substitutionValues: substitutionValues,
      ));
      final r = await body();
      _eventSink.add(PgPoolEvent(
        connectionId: connectionId,
        sessionId: sessionId,
        traceId: traceId,
        action: PgPoolAction.queryCompleted,
        query: query,
        substitutionValues: substitutionValues,
        elapsed: sw.elapsed,
      ));
      return r;
    } catch (e, st) {
      _eventSink.add(PgPoolEvent(
        connectionId: connectionId,
        sessionId: sessionId,
        traceId: traceId,
        action: PgPoolAction.queryFailed,
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
  void cancelTransaction({String? reason}) {
    _delegate.cancelTransaction(reason: reason);
  }

  @override
  Future<int> execute(String fmtString,
      {Map<String, dynamic>? substitutionValues, int? timeoutInSeconds}) {
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
      {Map<String, dynamic>? substitutionValues,
      bool? allowReuse = true,
      int? timeoutInSeconds}) {
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
      {Map<String, dynamic>? substitutionValues,
      bool? allowReuse = true,
      int? timeoutInSeconds}) {
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
