SELECT "foo"."track_id", COUNT("foo"."track_id") AS count
FROM (
    SELECT "plays"."track_id", "plays"."created_at"
    FROM "plays"
    WHERE
      ("plays"."created_at" >= timestamp ?)
      AND ("plays"."created_at" < timestamp ?)
) AS foo
GROUP BY "foo"."track_id";
