LUFFER
======

PhishTracks Stats data exporting tool.

Usage
-----

### Sunday 2018-09-23

This project is on the brink of disaster.

### Saturday 2018-09-22

```
# edit HOST var in provision.sh
# edit vars in pts/davinci/env/prod/luffer.env.gpg
./provision.sh
```

notes
- cli interface is outdated.

full instructions.
```
# use pts project to create the luffer box in terraform.
#localhost
./provision.sh
# ssh onto remote box
docker run -it --rm --net=host --name=luffer --env-file=../luffer.env luffer
lein> (ns luffer.worker2)
lein> (luffer.models/populate-model-cache!)
lein> (run-workers 4 (fn [w i] (->> w luffer.work/to-pts-docs (luffer.work/bulk-index-docs "plays.c.2018-09-22.offline" "play" 1000))))
```


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
export ES_URL='http://localhost:9202'
export DATABASE_URL='<read only slave>'
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
