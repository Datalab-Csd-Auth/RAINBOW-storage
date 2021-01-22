package org.auth.csd.datalab;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.ServiceContext;
import org.auth.csd.datalab.common.helpers.Message;
import org.auth.csd.datalab.common.helpers.Metric;
import org.auth.csd.datalab.common.interfaces.DataInterface;
import org.json.JSONArray;
import org.json.JSONObject;
import org.rapidoid.buffer.Buf;
import org.rapidoid.commons.Str;
import org.rapidoid.http.AbstractHttpServer;
import org.rapidoid.http.HttpStatus;
import org.rapidoid.http.HttpUtils;
import org.rapidoid.net.Server;
import org.rapidoid.net.abstracts.Channel;
import org.rapidoid.net.impl.RapidoidHelper;

import java.io.IOException;
import java.util.*;


public class DataService implements DataInterface {

    //TODO ENV variables
    private String cacheName = "Monitoring";
    @IgniteInstanceResource
    private Ignite ignite;
    /** Reference to the cache. */
    private IgniteCache<String, String> myCache;
    UUID localNode = null;
    private String delimiter = ".";
    private Server server;

    /** {@inheritDoc} */
    public void init(ServiceContext ctx) {
        System.out.println("Initializing Data Service on node:" + ignite.cluster().localNode());
        /**
         * It's assumed that the cache has already been deployed. To do that, make sure to start Data Nodes with
         * a respective cache configuration.
         */
        myCache = ignite.cache(cacheName);
        localNode = ignite.cluster().localNode().id();
        server = new CustomHttpServer().listen(50000);
    }

    /** {@inheritDoc} */
    public void execute(ServiceContext ctx) {
        System.out.println("Executing Data Service on node:" + ignite.cluster().localNode());
    }

    /** {@inheritDoc} */
    public void cancel(ServiceContext ctx) {
        System.out.println("Stopping Data Service on node:" + ignite.cluster().localNode());
        server.shutdown();
    }

    @Override
    public void ingestData(HashMap<String, Metric> data) {
        for (Map.Entry<String, Metric> entry : data.entrySet()) {
            myCache.put(entry.getKey(), entry.getValue().toString());
        }
    }

    private IgniteBiPredicate<String, String> createExtractionFilter(HashSet<String> search){
        //Create filter
        IgniteBiPredicate<String, String> filter = (key, val) -> {
            boolean res = false;
            for(String tmp: search) {
                res = key.startsWith(tmp);
                if(res) break;
            }
            return res;
        };
        return filter;
    }

    @Override
    public HashSet<String> extractData(HashSet<String> search) {
        HashSet<String> data = new HashSet<>();
        IgniteBiPredicate<String, String> filter = createExtractionFilter(search);
        myCache.query(new ScanQuery<>(filter)).forEach(entry -> {
            data.add(entry.getValue());
        });
        return data;
    }

    private class CustomHttpServer extends AbstractHttpServer {

        private final byte[] URI_PUT = "/put".getBytes();

        private final byte[] URI_GET = "/get".getBytes();

        @Override
        protected HttpStatus handle(Channel ctx, Buf buf, RapidoidHelper req) {
            if(!req.isGet.value){
                if (matches(buf, req.path, URI_PUT)) {
                    //Data structure for metrics
                    HashMap<String, Metric> metrics = new HashMap<>();
                    //Read and parse json
                    String body = buf.get(req.body);
                    JSONObject obj = new JSONObject(body);
                    //Get the monitoring keyword
                    JSONArray monitor = obj.getJSONArray("monitoring");
                    for (int i = 0; i< monitor.length(); i++){
                        JSONObject o = monitor.getJSONObject(i);
                        ObjectMapper m = new ObjectMapper();
                        try {
                            Metric myMetric = m.readValue(o.toString(), Metric.class);
                            String key = myMetric.entityID+delimiter+myMetric.metricID+delimiter+ myMetric.timestamp;
                            metrics.put(key, myMetric);
                        } catch (IOException e) {
                            e.printStackTrace();
                            return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR","Error on json schema!"));
                        }
                    }
                    ingestData(metrics);
                    return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("OK", "Ingestion successful!"));
                }else if (matches(buf, req.path, URI_GET)) {
                    //Data structure for metrics
                    HashSet<String> search = new HashSet<>();
                    //Read and parse json
                    String body = buf.get(req.body);
                    if(!body.equals("")) { //Check if body has filters
                        JSONObject obj = new JSONObject(body);
                        try {
                            JSONArray entities = obj.getJSONArray("entityID");
                            JSONArray metrics = obj.getJSONArray("metricID");
                            for (Object ent : entities) {
                                String myEnt = ent.toString();
                                for (Object metric : metrics)
                                    search.add(myEnt + delimiter + metric.toString());
                            }
                        }catch (Exception e){
                            e.printStackTrace();
                            return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Error on data extraction!"));
                        }
                    }
                    HashSet<String> result = extractData(search);
                    String join = String.join(", ", result);
                    join = "["+join+"]";
                    return json(ctx, req.isKeepAlive.value, join.getBytes());
                }
            }
            return HttpStatus.NOT_FOUND;
        }
    }

}
