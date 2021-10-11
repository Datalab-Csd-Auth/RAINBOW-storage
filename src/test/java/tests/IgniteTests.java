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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Collection;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class IgniteTests {

    @BeforeAll
    public static void startServer() throws IgniteCheckedException {
        // Start ignite server
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
    public void CacheCreation() throws Exception {
        //Connect thin client
        try (IgniteClient client = createThinClient()) {
            Collection<String> caches = client.cacheNames();
            Assertions.assertTrue(caches.contains("LatestMonitoring"));
            Assertions.assertTrue(caches.contains("HistoricalMonitoring"));
            Assertions.assertTrue(caches.contains("MetaMonitoring"));
        }
    }

    @Test
    @Order(2)
    public void AddMetric() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        //Input json
        String json = "{\"monitoring\": [ { \"entityID\": \"ent1\", \"entityType\": \"fog\", \"metricID\": \"metr1\", \"name\": \"ram\", \"units\": \"gb\", \"desc\": \"RAM\", \"group\": \"fog_group\", \"minVal\": 5, \"maxVal\": 100, \"higherIsBetter\": false, \"val\": 6, \"timestamp\": 1611318068003 } ] }";
        //Create the request
        StringEntity entity = new StringEntity(json);
        HttpPost httpPost = new HttpPost("http://localhost:50000/put");
        httpPost.setEntity(entity);
        //Get the response
        HttpResponse httpResponse = HttpClientBuilder.create().build().execute( httpPost );
        String jsonFromResponse = EntityUtils.toString(httpResponse.getEntity());
        //Assert against ground truth
        String truth = "{\"result\":\"OK\",\"message\":\"Ingestion successful!\"}";
        Assertions.assertEquals(mapper.readTree(jsonFromResponse), mapper.readTree(truth));
    }

    @Test
    @Order(3)
    public void GetLatestMetric() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        //Input json
        String json = "{\"metricID\": [\"metr1\"], \"latest\": true }";
        //Create the request
        StringEntity entity = new StringEntity(json);
        HttpPost httpPost = new HttpPost("http://localhost:50000/get");
        httpPost.setEntity(entity);
        //Get the response
        HttpResponse httpResponse = HttpClientBuilder.create().build().execute( httpPost );
        String jsonFromResponse = EntityUtils.toString(httpResponse.getEntity());
        //Assert against ground truth
        String truth = "{\"monitoring\":[{\"metricID\":\"metr1\",\"val\":6.0,\"timestamp\":1611318068003,\"entityID\":\"ent1\",\"entityType\":\"fog\",\"name\":\"ram\",\"units\":\"gb\",\"desc\":\"RAM\",\"group\":\"fog_group\",\"minVal\":5.0,\"maxVal\":100.0,\"higherIsBetter\":false}]}";
        Assertions.assertEquals(mapper.readTree(jsonFromResponse), mapper.readTree(truth));
    }

    @Test
    @Order(4)
    public void GetHistoricalMetric() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        //Input json
        String json = "{\"metricID\": [\"metr1\"], \"from\": 1611318068000, \"to\": 1611318068010, \"latest\": false }";
        //Create the request
        StringEntity entity = new StringEntity(json);
        HttpPost httpPost = new HttpPost("http://localhost:50000/get");
        httpPost.setEntity(entity);
        //Get the response
        HttpResponse httpResponse = HttpClientBuilder.create().build().execute( httpPost );
        String jsonFromResponse = EntityUtils.toString(httpResponse.getEntity());
        //Assert against ground truth
        String truth = "{\"monitoring\": [  { \"metricID\": \"metr1\" , \"values\": [  { \"timestamp\": 1611318068003 , \"val\": 6.0 }  ]  ,  \"entityID\": \"ent1\", \"entityType\": \"fog\", \"name\": \"ram\", \"units\": \"gb\", \"desc\": \"RAM\", \"group\": \"fog_group\", \"minVal\": 5.0, \"maxVal\": 100.0, \"higherIsBetter\": false  }  ]}";
        Assertions.assertEquals(mapper.readTree(jsonFromResponse), mapper.readTree(truth));
    }

    @Test
    @Order(5)
    public void NonExistentMetric() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        //Input json
        String json = "{\"metricID\": [\"metr2\"], \"latest\": true }";
        //Create the request
        StringEntity entity = new StringEntity(json);
        HttpPost httpPost = new HttpPost("http://localhost:50000/get");
        httpPost.setEntity(entity);
        //Get the response
        HttpResponse httpResponse = HttpClientBuilder.create().build().execute( httpPost );
        String jsonFromResponse = EntityUtils.toString(httpResponse.getEntity());
        //Assert against ground truth
        String truth = "{\"monitoring\": [  ]} ";
        Assertions.assertEquals(mapper.readTree(jsonFromResponse), mapper.readTree(truth));
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
