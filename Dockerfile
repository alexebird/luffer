FROM clojure
MAINTAINER Alex Bird <alexebird@gmail.com>
WORKDIR /tmp
COPY project.clj ./
RUN lein deps
RUN rm project.clj
WORKDIR /root
RUN git clone https://github.com/alexebird/luffer.git
WORKDIR /root/luffer
RUN ln -s "$(lein uberjar | sed -n 's/^Created \(.*standalone\.jar\)/\1/p')" luffer-standalone.jar
WORKDIR /root
CMD ["java", "-jar", "luffer-standalone.jar"]
