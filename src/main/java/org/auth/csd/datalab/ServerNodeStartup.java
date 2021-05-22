package org.auth.csd.datalab;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.auth.csd.datalab.common.filter.DataFilter;
import org.auth.csd.datalab.common.models.*;
import org.auth.csd.datalab.services.DataService;

import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.auth.csd.datalab.common.Helpers.readEnvVariable;
import static org.auth.csd.datalab.common.interfaces.DataInterface.SERVICE_NAME;

/**
 * A new Data Node will be started in a separate JVM process when this class gets executed.
 */
public class ServerNodeStartup {

    public static final Boolean persistence = readEnvVariable("PERSISTENCE") != null && Boolean.parseBoolean(readEnvVariable("PERSISTENCE"));
    public static final Boolean app_cache = readEnvVariable("APP_CACHE") != null && Boolean.parseBoolean(readEnvVariable("APP_CACHE"));
    public static final String latestCacheName = "LatestMonitoring";
    public static final String historicalCacheName = "HistoricalMonitoring";
    public static final String metaCacheName = "MetaMonitoring";
    public static final String appCacheName = "ApplicationData";
    public static final String persistenceRegion = "Persistent_Region";
    public static  int evictionHours = 168;

    public static void createServer(String discovery, String hostname) throws IgniteException {
        Ignite ignite = Ignition.start(igniteConfiguration(discovery,hostname));
        ignite.cluster().state(ClusterState.ACTIVE);
        ignite.cluster().baselineAutoAdjustEnabled(true);
        ignite.cluster().baselineAutoAdjustTimeout(10);
        System.out.println(ignite.cluster().localNode().id());
    }

    private static IgniteConfiguration igniteConfiguration(String discovery, String hostname) {
        System.setProperty("java.net.preferIPv4Stack", "true");
        Map<String, Boolean> myAtrr = new HashMap<>();
        myAtrr.put("data.node", true);
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setPeerClassLoadingEnabled(true);
        cfg.setUserAttributes(myAtrr);
        cfg.setLocalHost(hostname);
        cfg.setDiscoverySpi(new TcpDiscoverySpi()
                .setIpFinder(new TcpDiscoveryVmIpFinder().setAddresses(Arrays.asList(discovery.split(NodeStartup.discoveryDelimiter))))
        );


        if(persistence){
            try {
                String evict = readEnvVariable("EVICTION");
                if (evict != null) evictionHours = Integer.parseInt(Objects.requireNonNull(readEnvVariable("EVICTION")));
            } catch (NumberFormatException e) {
                System.out.println("Input Eviction value cannot be parsed to Integer. Default value will be used.");
            }
            DataStorageConfiguration dsc = new DataStorageConfiguration();
            DataRegionConfiguration regionWithPersistence = new DataRegionConfiguration();
            regionWithPersistence.setName(persistenceRegion);
            regionWithPersistence.setPersistenceEnabled(true);
            dsc.setDataRegionConfigurations(regionWithPersistence);
            cfg.setDataStorageConfiguration(dsc);
        }

        CacheConfiguration<String, TimedMetric> latestCfg = new CacheConfiguration<>(latestCacheName);
        latestCfg.setCacheMode(CacheMode.LOCAL);

        CacheConfiguration<MetaMetricKey, MetaMetric> metaCfg = new CacheConfiguration<>(metaCacheName);
        metaCfg.setCacheMode(CacheMode.LOCAL);
        metaCfg.setIndexedTypes(MetaMetricKey.class, MetaMetric.class);
        if(persistence) metaCfg.setDataRegionName(persistenceRegion)
                .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.HOURS, evictionHours)))
                .setEagerTtl(true);

        CacheConfiguration<MetricKey, Metric> historicalCfg = new CacheConfiguration<>(historicalCacheName);
        historicalCfg.setCacheMode(CacheMode.LOCAL);
        historicalCfg.setIndexedTypes(MetricKey.class, Metric.class);
        if(persistence) historicalCfg.setDataRegionName(persistenceRegion)
                .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.HOURS, evictionHours)))
                .setEagerTtl(true);


        if(app_cache) {
            cfg.setCacheConfiguration(latestCfg, metaCfg, historicalCfg,
                    new CacheConfiguration<>(appCacheName)
                            .setCacheMode(CacheMode.LOCAL)
            );
        }else{
            cfg.setCacheConfiguration(latestCfg, metaCfg, historicalCfg);
        }
        cfg.setServiceConfiguration(serviceConfiguration());

        return cfg;
    }

    private static ServiceConfiguration serviceConfiguration() {
        // Gives back a Node Singleton Service
        ServiceConfiguration cfg = new ServiceConfiguration();
        cfg.setName(SERVICE_NAME);
        cfg.setMaxPerNodeCount(1);
        cfg.setNodeFilter(new DataFilter());
        cfg.setService(new DataService());
        return cfg;
    }

}
