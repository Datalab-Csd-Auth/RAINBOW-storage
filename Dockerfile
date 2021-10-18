FROM --platform=$TARGETPLATFORM openjdk:8u302-jre
ARG TARGETPLATFORM
COPY /target/*.jar /opt/assets/ignite.jar
ENV JAVA_OPTS_GC="-server -XX:+AlwaysPreTouch -XX:+UseG1GC -XX:+ScavengeBeforeFullGC -XX:+DisableExplicitGC"
ENV JAVA_OPTS="-Xms256m -Xmx256m"
WORKDIR /opt/assets
EXPOSE 50000
EXPOSE 50001
EXPOSE 10800 11211 47100 47500 49112 8080
CMD java $JAVA_OPTS_GC $JAVA_OPTS -jar ignite.jar
