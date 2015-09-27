FROM clojure
MAINTAINER Alex Bird <alexebird@gmail.com>

WORKDIR /root
COPY ./target/uberjar/luffer-0.1.0-standalone.jar ./luffer-standalone.jar
ENTRYPOINT ["java", "-jar", "luffer-standalone.jar"]
CMD ["java", "-jar", "luffer-standalone.jar"]
