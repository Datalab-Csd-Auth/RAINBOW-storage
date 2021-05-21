FROM openjdk:8u212-jre-alpine
COPY assets/ /opt/assets
EXPOSE 50000
EXPOSE 50001
EXPOSE 10800 11211 47100 47500 49112 8080
CMD ["java", "-jar", "/opt/assets/ignite.jar"]
