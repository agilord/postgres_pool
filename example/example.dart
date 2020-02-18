import 'package:postgres_pool/postgres_pool.dart';

Future<void> main() async {
  final pg = PgPool(
    PgUrl(
        host: 'localhost',
        port: 5432,
        database: 'test',
        username: 'test',
        password: 'test'),
  );

  final futures = <Future>[];
  for (var i = 0; i < 100; i++) {

    // pg.run schedules a callback function to be called when a connection
    // becomes available. We are not blocking on the result yet.
    final f = pg.run((c) async {
      final rs = await c.query(
        'SELECT COUNT(*) FROM test_table WHERE type = @type',
        substitutionValues: {
          'type': i,
        },
      );
      return rs[0][0];
    });

    futures.add(f);
  }

  // We've scheduled 100 connection-using callbacks, and very likely the first
  // one is already ongoing. Let's wait for all of them.
  await Future.wait(futures);

  await pg.close();
}
