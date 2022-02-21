package tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.auth.csd.datalab.ServerNodeStartup;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Collection;


@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class IgniteTests {

    private final String jsonPut = "{\"monitoring\": [ { \"entityID\": \"ent1\", \"entityType\": \"fog\", \"metricID\": \"metr1\", \"name\": \"ram\", \"units\": \"gb\", \"desc\": \"RAM\", \"group\": \"fog_group\", \"minVal\": 5, \"maxVal\": 100, \"higherIsBetter\": false, \"val\": 6, \"timestamp\": 1611318068003 } ] }";
    private final String jsonLatest = "{\"metricID\": [\"metr1\"], \"latest\": true }";
    private final String jsonHistorical = "{\"metricID\": [\"metr1\"], \"from\": 1611318068000, \"to\": 1611318068010, \"latest\": false }";
    private final String jsonNull = "{\"metricID\": [\"metr2\"], \"latest\": true }";



    //In order for the tests to run correctly the node should start in CLUSTER_HEAD mode.
    //This means that the env variable should be set (CLUSTER_HEAD=true)
    @BeforeAll
    public static void startServer() throws IgniteCheckedException {
        // Start ignite server
        System.setProperty("CLUSTER_HEAD", "true");
        ServerNodeStartup.createServer("localhost","localhost");
        waitForPort("localhost", 50000, 30000);
    }

    @AfterAll
    public static void stopServer(){
        // Start ignite server
        Ignition.stopAll(true);
    }

    @Test
    @Order(1)
    void CacheCreation() throws Exception {
        //Connect thin client
        try (IgniteClient client = createThinClient()) {
            Collection<String> caches = client.cacheNames();
            Assertions.assertTrue(caches.contains(ServerNodeStartup.LATEST_CACHE_NAME));
            Assertions.assertTrue(caches.contains(ServerNodeStartup.HISTORICAL_CACHE_NAME));
            Assertions.assertTrue(caches.contains(ServerNodeStartup.META_CACHE_NAME));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {jsonPut})
    @Order(2)
    void AddMetric(String arg) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        //Create the request
        StringEntity entity = new StringEntity(arg);
        HttpPost httpPost = new HttpPost("http://localhost:50000/put");
        httpPost.setEntity(entity);
        //Get the response
        HttpResponse httpResponse = HttpClientBuilder.create().build().execute( httpPost );
        String jsonFromResponse = EntityUtils.toString(httpResponse.getEntity());
        //Assert against ground truth
        Assertions.assertEquals(mapper.readTree(jsonFromResponse), mapper.readTree(getResponse(arg)));
    }


    @ParameterizedTest
    @ValueSource(strings = {jsonLatest, jsonHistorical, jsonNull})
    @Order(3)
    void GetLatestMetric(String arg) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        //Create the request
        StringEntity entity = new StringEntity(arg);
        HttpPost httpPost = new HttpPost("http://localhost:50000/get");
        httpPost.setEntity(entity);
        //Get the response
        HttpResponse httpResponse = HttpClientBuilder.create().build().execute( httpPost );
        String jsonFromResponse = EntityUtils.toString(httpResponse.getEntity());
        //Assert against ground truth
        Assertions.assertEquals(mapper.readTree(jsonFromResponse), mapper.readTree(getResponse(arg)));
    }

    private String getResponse(String arg){
        switch (arg){
            case jsonPut: return "{\"result\":\"OK\",\"message\":\"Ingestion successful!\"}";
            case jsonLatest: return "{\"monitoring\":[{\"node\":\"localhost\",\"data\":[{\"metricID\":\"metr1\",\"entityID\":\"ent1\",\"values\":[{\"timestamp\":1611318068003,\"val\":6.0}],\"entityType\":\"fog\",\"name\":\"ram\",\"units\":\"gb\",\"desc\":\"RAM\",\"group\":\"fog_group\",\"minVal\":5.0,\"maxVal\":100.0,\"higherIsBetter\":false,\"pod\":{\"uuid\":\"null\",\"name\":\"null\",\"namespace\":\"null\"},\"container\":{\"id\":\"null\",\"name\":\"null\"}}]}]}";
            case jsonHistorical: return "{\"monitoring\":[{\"node\":\"localhost\",\"data\":[{\"metricID\":\"metr1\",\"entityID\":\"ent1\",\"values\":[{\"timestamp\":1611318068003,\"val\":6.0}],\"entityType\":\"fog\",\"name\":\"ram\",\"units\":\"gb\",\"desc\":\"RAM\",\"group\":\"fog_group\",\"minVal\":5.0,\"maxVal\":100.0,\"higherIsBetter\":false,\"pod\":{\"uuid\":\"null\",\"name\":\"null\",\"namespace\":\"null\"},\"container\":{\"id\":\"null\",\"name\":\"null\"}}]}]}";
            case jsonNull: return "{\"monitoring\":[{\"node\":\"localhost\",\"data\":[]}]}";
        }
        return null;
    }

    private IgniteClient createThinClient(){
        ClientConfiguration cfg = new ClientConfiguration().setAddresses("localhost:10800");
        return Ignition.startClient(cfg);
    }

    private static void waitForPort(String hostname, int port, long timeoutMs) {
        long startTs = System.currentTimeMillis();
        boolean scanning=true;
        while(scanning)
        {
            if (System.currentTimeMillis() - startTs > timeoutMs) {
                throw new RuntimeException("Timeout waiting for port " + port);
            }
            try
            {
                SocketAddress addr = new InetSocketAddress(hostname, port);
                Selector.open();
                SocketChannel socketChannel = SocketChannel.open();
                socketChannel.configureBlocking(true);
                try {
                    socketChannel.connect(addr);
                }
                finally {
                    socketChannel.close();
                }

                scanning=false;
            }
            catch(IOException e)
            {
                try
                {
                    Thread.sleep(2000);//2 seconds
                }
                catch(InterruptedException ie){
                    System.out.println("Thread sleep error");
                }
            }
        }
    }

}
