package org.auth.csd.datalab;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;

import static org.auth.csd.datalab.common.Helpers.readEnvVariable;

public class InfluxConnector {

    private static String influxHost = (readEnvVariable("INFLUX_HOST") != null) ? readEnvVariable("INFLUX_HOST") : "http://localhost:8086";
    private static char[] token = "my-super-secret-auth-token".toCharArray();
    private static String org = "my_org";
    private static String bucket = "my_bucket";
    private static String nodeMeasurement = "node";
    private static String nodeTagHost = "hostname";
    private static String nodeTagStatus = "status";
    private static String scoreMeasurement = "score";
    private static String scoreTagHost = "hostname";
    private static String scoreTagStatus = "status";
    private static String replicaMeasurement = "replicate";
    private static String replicaTagFrom = "from";
    private static String replicaTagTo = "to";
    private static String replicaTagStatus = "status";
    private static String value = "value";

    public static class WriteData{

        public static void writeNodeStart(String hostname, long timestamp){
            InfluxDBClient influxDBClient = InfluxDBClientFactory.create(influxHost, token, org, bucket);
            WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
            Point point = Point.measurement(nodeMeasurement)
                    .addTag(nodeTagHost, hostname)
                    .addTag(nodeTagStatus, "start")
                    .addField(value, 0)
                    .time(timestamp, WritePrecision.MS);
            writeApi.writePoint(point);
            influxDBClient.close();
        }

        public static void writeNodeRestart(String hostname, long timestamp){
            InfluxDBClient influxDBClient = InfluxDBClientFactory.create(influxHost, token, org, bucket);
            WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
            Point point = Point.measurement(nodeMeasurement)
                    .addTag(nodeTagHost, hostname)
                    .addTag(nodeTagStatus, "restart")
                    .addField(value, 1)
                    .time(timestamp, WritePrecision.MS);
            writeApi.writePoint(point);
            influxDBClient.close();
        }

        public static void writeNodeFail(String hostname, long timestamp){
            InfluxDBClient influxDBClient = InfluxDBClientFactory.create(influxHost, token, org, bucket);
            WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
            Point point = Point.measurement(nodeMeasurement)
                    .addTag(nodeTagHost, hostname)
                    .addTag(nodeTagStatus, "fail")
                    .addField(value, 2)
                    .time(timestamp, WritePrecision.MS);
            writeApi.writePoint(point);
            influxDBClient.close();
        }

        public static void writeUnstableScore(String hostname, long timestamp, double score){
            InfluxDBClient influxDBClient = InfluxDBClientFactory.create(influxHost, token, org, bucket);
            WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
            Point point = Point.measurement(scoreMeasurement)
                    .addTag(scoreTagHost, hostname)
                    .addTag(scoreTagStatus, "unstable")
                    .addField(value, score)
                    .time(timestamp, WritePrecision.MS);
            writeApi.writePoint(point);
            influxDBClient.close();
        }

        public static void writeStableScore(String hostname, long timestamp, double score){
            InfluxDBClient influxDBClient = InfluxDBClientFactory.create(influxHost, token, org, bucket);
            WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
            Point point = Point.measurement(scoreMeasurement)
                    .addTag(scoreTagHost, hostname)
                    .addTag(scoreTagStatus, "stable")
                    .addField(value, score)
                    .time(timestamp, WritePrecision.MS);
            writeApi.writePoint(point);
            influxDBClient.close();
        }

        public static void writeReplicaStart(String from, String to, long timestamp){
            InfluxDBClient influxDBClient = InfluxDBClientFactory.create(influxHost, token, org, bucket);
            WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
            Point point = Point.measurement(replicaMeasurement)
                    .addTag(replicaTagFrom, from)
                    .addTag(replicaTagTo, to)
                    .addTag(replicaTagStatus, "start")
                    .addField(value, 1)
                    .time(timestamp, WritePrecision.MS);
            writeApi.writePoint(point);
            influxDBClient.close();
        }

        public static void writeReplicaStop(String from, String to, long timestamp){
            InfluxDBClient influxDBClient = InfluxDBClientFactory.create(influxHost, token, org, bucket);
            WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
            Point point = Point.measurement(replicaMeasurement)
                    .addTag(replicaTagFrom, from)
                    .addTag(replicaTagTo, to)
                    .addTag(replicaTagStatus, "stop")
                    .addField(value, 0)
                    .time(timestamp, WritePrecision.MS);
            writeApi.writePoint(point);
            influxDBClient.close();
        }

    }
}
