SELECT "foo"."track_id", COUNT("foo"."track_id") AS count
FROM (
    SELECT "plays"."track_id", "plays"."created_at"
    FROM "plays"
    WHERE
      ("plays"."created_at" >= timestamp '2017-05-13T00:00:00Z')
      AND ("plays"."created_at" < timestamp '2017-05-13T06:00:00Z')
) AS foo
GROUP BY "foo"."track_id";
