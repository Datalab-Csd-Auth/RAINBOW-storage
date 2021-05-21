FROM openjdk:8u212-jre-alpine
COPY assets/ /opt/assets
#RUN cp /opt/assets/*.jar /opt/ignite/apache-ignite/libs
#ENV OPTION_LIBS=ignite-zookeeper
#ENV CONFIG_URI=/opt/assets/config/ignite-server.xml
EXPOSE 50000
EXPOSE 50001
EXPOSE 10800 11211 47100 47500 49112 8080
CMD ["java", "-jar", "/opt/assets/Ignite-Server-1.2.2-jar-with-dependencies.jar"]
