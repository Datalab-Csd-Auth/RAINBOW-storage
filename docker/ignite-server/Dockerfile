FROM apacheignite/ignite:2.9.0
COPY assets/ /opt/assets
RUN cp /opt/assets/*.jar /opt/ignite/apache-ignite/libs
ENV OPTION_LIBS=ignite-zookeeper
ENV CONFIG_URI=/opt/assets/config/ignite-server.xml
EXPOSE 50000
EXPOSE 50001
