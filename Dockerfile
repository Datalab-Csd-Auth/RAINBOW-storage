FROM --platform=$TARGETPLATFORM openjdk:8u302-jre
ARG TARGETPLATFORM
COPY /target/*.jar /opt/assets/ignite.jar
WORKDIR /opt/assets
EXPOSE 50000
EXPOSE 50001
EXPOSE 10800 11211 47100 47500 49112 8080
CMD ["java", "-jar", "ignite.jar"]
