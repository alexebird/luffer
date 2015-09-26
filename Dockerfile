FROM clojure
MAINTAINER Alex Bird <alexebird@gmail.com>

WORKDIR /usr/src/luffer
RUN git clone https://github.com/alexebird/luffer.git
#COPY project.clj /usr/src/app/
#RUN lein deps
#COPY . /usr/src/app
#RUN mv "$(lein uberjar | sed -n 's/^Created \(.*standalone\.jar\)/\1/p')" luffer-standalone.jar
#CMD ["java", "-jar", "luffer-standalone.jar"]
