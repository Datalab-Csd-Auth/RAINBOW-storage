package org.auth.csd.datalab;

import org.apache.ignite.*;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.*;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.auth.csd.datalab.common.filter.DataFilter;
import org.auth.csd.datalab.common.filter.HeadFilter;
import org.auth.csd.datalab.common.interfaces.RebalanceInterface;
import org.auth.csd.datalab.common.models.keys.*;
import org.auth.csd.datalab.common.models.values.MetaMetric;
import org.auth.csd.datalab.common.models.values.Metric;
import org.auth.csd.datalab.common.models.values.TimedMetric;
import org.auth.csd.datalab.services.*;

import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.auth.csd.datalab.common.Helpers.readEnvVariable;

/**
 * A new Data Node will be started in a separate JVM process when this class gets executed.
 */
public class ServerNodeStartup {

    //Get env variables
    public static final Boolean cluster_head = readEnvVariable("CLUSTER_HEAD") != null && Boolean.parseBoolean(readEnvVariable("CLUSTER_HEAD"));
    public static final Boolean app_cache = readEnvVariable("APP_CACHE") != null && Boolean.parseBoolean(readEnvVariable("APP_CACHE"));
    //Monitoring caches
    public static final String latestCacheName = "LatestMonitoring";
    public static final String historicalCacheName = "HistoricalMonitoring";
    public static final String metaCacheName = "MetaMonitoring";
    //Analytics cache
    public static final String analyticsCacheName = "Analytics";
    //Local consistent id
    public static String localNode = null;
    //Replicas monitoring caches
    public static final String replicaLatestCacheName = "ReplicaLatestMonitoring";
    public static final String replicaHistoricalCacheName = "ReplicaHistoricalMonitoring";
    public static final String replicaMetaCacheName = "ReplicaMetaMonitoring";
    public static final String replicaHostCache = "Replicas";
    //Optional application cache
    public static final String appCacheName = "ApplicationData";
    private static int evictionHours = 168;
    private static long totalSize = 512;
    private static final String defaultRegionName = "Default_Region";
    private static final String persistenceRegionName = "Persistent_Region";

    public static void createServer(String discovery, String hostname) throws IgniteException {
        Ignite ignite = Ignition.start(igniteConfiguration(discovery, hostname));
        ignite.cluster().state(ClusterState.ACTIVE);
        ignite.cluster().baselineAutoAdjustEnabled(true);
        ignite.cluster().baselineAutoAdjustTimeout(60000);
        localNode = hostname;
        System.out.println("Local node: " + localNode);
    }

    private static IgniteConfiguration igniteConfiguration(String discovery, String hostname) {
        //Set cluster identification and custom parameters
        //Get eviction rate
        String tmpEvict = readEnvVariable("EVICTION");
        if (tmpEvict != null) evictionHours = Integer.parseInt(tmpEvict);
        //Get total size of data regions
        String tmpSize = readEnvVariable("SIZE");
        if (tmpSize != null) totalSize = Long.parseLong(tmpSize);
        //Set attributes
        Map<String, Boolean> myAtrr = new HashMap<>();
        myAtrr.put("data.node", true);
        myAtrr.put("data.head", cluster_head);
        //Create context
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setPeerClassLoadingEnabled(true);
        cfg.setUserAttributes(myAtrr);
        cfg.setLocalHost(hostname);
        cfg.setDiscoverySpi(new TcpDiscoverySpi()
                .setIpFinder(new TcpDiscoveryVmIpFinder().setAddresses(Arrays.asList(discovery.split(NodeStartup.discoveryDelimiter))))
        );
        //Add the data regions to context
        cfg.setDataStorageConfiguration(dataStorageConfiguration());
        //Cache configurations
        //Latest monitoring data cache
        CacheConfiguration<HostMetricKey, TimedMetric> latestCfg = new CacheConfiguration<>(latestCacheName);
        latestCfg.setCacheMode(CacheMode.LOCAL)
                .setIndexedTypes(HostMetricKey.class, TimedMetric.class)
                .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.HOURS, evictionHours)))
                .setEagerTtl(true);
        //Historical monitoring data cache
        CacheConfiguration<HostMetricTimeKey, Metric> historicalCfg = new CacheConfiguration<>(historicalCacheName);
        historicalCfg.setCacheMode(CacheMode.LOCAL)
                .setIndexedTypes(HostMetricTimeKey.class, Metric.class)
                .setDataRegionName(persistenceRegionName)
                .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.HOURS, evictionHours)))
                .setEagerTtl(true);
        //Metadata cache (metric meta)
        //Create near cache for local reads
        NearCacheConfiguration<HostMetricKey, MetaMetric> nearCfg = new NearCacheConfiguration<>();
        nearCfg.setNearStartSize(50 * 1024 * 1024);
        //Create meta cache with near configs
        CacheConfiguration<HostMetricKey, MetaMetric> metaCfg = new CacheConfiguration<>(metaCacheName);
        metaCfg.setCacheMode(CacheMode.PARTITIONED)
                .setIndexedTypes(HostMetricKey.class, MetaMetric.class)
                .setBackups(1)
                .setNearConfiguration(nearCfg)
                .setDataRegionName(persistenceRegionName);
        //Analytics cache
        CacheConfiguration<AnalyticKey, Metric> analyticsCfg = new CacheConfiguration<>(analyticsCacheName);
        analyticsCfg.setCacheMode(CacheMode.LOCAL)
                .setIndexedTypes(AnalyticKey.class, Metric.class)
                .setDataRegionName(persistenceRegionName)
                .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.HOURS, evictionHours)))
                .setEagerTtl(true);
        //Optional application k-v cache
        if (app_cache) {
            cfg.setCacheConfiguration(latestCfg, metaCfg, historicalCfg, analyticsCfg,
                    new CacheConfiguration<AnalyticKey, Metric>(appCacheName)
                            .setCacheMode(CacheMode.LOCAL)
                            .setIndexedTypes(AnalyticKey.class, Metric.class)
                            .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.HOURS, evictionHours)))
                            .setEagerTtl(true)
            );
        } else {
            cfg.setCacheConfiguration(latestCfg, metaCfg, historicalCfg, analyticsCfg);
        }
        //Activate services on nodes
        cfg.setServiceConfiguration(httpServiceConfiguration(), dataMngmServiceConfiguration(), movementServiceConfiguration(), rebalanceServiceConfiguration());

        /*
        //=============DEBUG Logging
        IgniteLogger log = new Log4JLogger("ignite-log4j.xml");
        cfg.setGridLogger(log);

        //=============DEBUG Metrics logging
        LogExporterSpi logExporter = new LogExporterSpi();
        logExporter.setPeriod(1000);
        cfg.setMetricExporterSpi(logExporter);
         */

        return cfg;
    }

    private static ServiceConfiguration httpServiceConfiguration() {
        //Gives back a Node Singleton Service
        ServiceConfiguration sCfg = new ServiceConfiguration();
        sCfg.setName(HttpService.SERVICE_NAME);
        sCfg.setMaxPerNodeCount(1);
        sCfg.setNodeFilter(new DataFilter());
        sCfg.setService(new HttpService());
        return sCfg;
    }

    private static ServiceConfiguration dataMngmServiceConfiguration() {
        //Gives back a Node Singleton Service
        ServiceConfiguration sCfg = new ServiceConfiguration();
        sCfg.setName(DataManagement.SERVICE_NAME);
        sCfg.setMaxPerNodeCount(1);
        sCfg.setNodeFilter(new DataFilter());
        sCfg.setService(new DataManagement());
        return sCfg;
    }

    private static ServiceConfiguration movementServiceConfiguration() {
        //Gives back a Node Singleton Service
        ServiceConfiguration sCfg = new ServiceConfiguration();
        sCfg.setName(MovementService.SERVICE_NAME);
        sCfg.setMaxPerNodeCount(1);
        sCfg.setNodeFilter(new DataFilter());
        sCfg.setService(new MovementService());
        return sCfg;
    }

    private static ServiceConfiguration rebalanceServiceConfiguration() {
        //Gives back a Node Singleton Service
        ServiceConfiguration sCfg = new ServiceConfiguration();
        sCfg.setName(RebalanceInterface.SERVICE_NAME);
        sCfg.setMaxPerNodeCount(1);
        sCfg.setNodeFilter(new HeadFilter());
        sCfg.setService(new RebalanceService());
        return sCfg;
    }

    private static DataStorageConfiguration dataStorageConfiguration() {
        //Split size between the 2 regions
        long regionSize = Math.max(totalSize / 2, 100);
        //Set data regions
        DataStorageConfiguration dsc = new DataStorageConfiguration();
        //Default in-memory region
        DataRegionConfiguration defaultRegion = new DataRegionConfiguration();
        defaultRegion.setName(defaultRegionName);
        defaultRegion.setInitialSize(100 * 1024 * 1024); //100 mb
        defaultRegion.setMaxSize(regionSize * 1024 * 1024); //default is 256 mb
        defaultRegion.setPageEvictionMode(DataPageEvictionMode.RANDOM_2_LRU);
        defaultRegion.setMetricsEnabled(true);
        dsc.setDefaultDataRegionConfiguration(defaultRegion);
        //Persistence region
        DataRegionConfiguration regionWithPersistence = new DataRegionConfiguration();
        regionWithPersistence.setName(persistenceRegionName);
        regionWithPersistence.setInitialSize(100 * 1024 * 1024); //100 mb
        regionWithPersistence.setMaxSize(regionSize * 1024 * 1024); //default is 256 mb
        regionWithPersistence.setPersistenceEnabled(true);
        regionWithPersistence.setMetricsEnabled(true);
        dsc.setDataRegionConfigurations(regionWithPersistence);
        return dsc;
    }

}
