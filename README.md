LUFFER
======

PhishTracks Stats data exporting tool.

Usage
-----

```
lein with-profiles -dev run -- -i plays.b.foo -c 4 -w dates
lein compile
lein uberjar
java -jar /home/bird/luffer/target/luffer-0.1.0-standalone.jar -i plays.b.foo -c 4 -w dates
```

`lein repl`

```
(run-workers 8 "ids" "plays.b.friday")
```

Then run the appropriate `plays_exporter.rb` in the `pts` repo.


License
-------

Copyright (c) 2014 Alexander Bird.

See LICENSE.
