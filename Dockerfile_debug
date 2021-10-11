FROM --platform=$TARGETPLATFORM maven:openjdk AS builder
ARG TARGETPLATFORM
ARG BUILDPLATFORM
COPY src /opt/src
COPY pom.xml /opt
WORKDIR /opt
RUN mvn -f /opt/pom.xml clean compile assembly:single

FROM --platform=$TARGETPLATFORM openjdk:8u302-jre
ARG TARGETPLATFORM
ARG BUILDPLATFORM
COPY --from=builder /opt/target/*.jar /opt/assets/ignite.jar
WORKDIR /opt/assets
EXPOSE 50000
EXPOSE 50001
EXPOSE 10800 11211 47100 47500 49112 8080
CMD ["java", "-jar", "ignite.jar"]
