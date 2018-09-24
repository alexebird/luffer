FROM clojure:lein-2.7.1
MAINTAINER Alex Bird <alexebird@gmail.com>

# hack to prevent null ptr ex during lein uberjar
ENV DATABASE_URL=""
ENV REDIS_URL=""
ENV ES_URL=""

RUN mkdir /root/luffer
WORKDIR /root/luffer

COPY project.clj .
RUN lein deps

COPY src src

RUN lein compile

CMD ["lein", "repl"]
