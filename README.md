LUFFER
======

PhishTracks Stats data exporting tool.

Usage
-----

2017-07-15:

```
(run-workers 2 (fn [w i] (->> w luffer.work/to-pts-docs (luffer.work/bulk-index-docs "plays.c.2017-07-15.offline" "play" 1000))))
```

2017-07-09:

```
# make sure the data base is accessible!
psql -Upts -hpg-jun-2017.alxb.us -p5432 -dpts_production
# get db password from:
gpg2 -d ~/pts/davinci/env/dev/dbs.sh.gpg
```

```
lein repl
(ns luffer.worker2)
(luffer.models/populate-model-cache!)
(run-workers 2 "plays.c.0" (fn [w i] (->> w luffer.work/to-pts-docs (luffer.work/bulk-index-docs "plays.c.0" "play" 500))))
```

old:

```
lein with-profiles -dev run -- -i plays.b.foo -c 4 -w dates
lein compile
lein uberjar
java -jar /home/bird/luffer/target/luffer-0.1.0-standalone.jar -i plays.b.foo -c 4 -w dates
```

```
lein repl
(run-workers 8 "ids" "plays.b.friday")
```

Then run the appropriate `plays_exporter.rb` in the `pts` repo.


License
-------

Copyright (c) 2014 Alexander Bird.

See LICENSE.
