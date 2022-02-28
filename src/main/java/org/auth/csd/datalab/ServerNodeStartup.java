package org.auth.csd.datalab;

import org.apache.ignite.*;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.EventType;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.auth.csd.datalab.common.filter.DataFilter;
import org.auth.csd.datalab.common.filter.HeadFilter;
import org.auth.csd.datalab.common.interfaces.DataManagementInterface;
import org.auth.csd.datalab.common.interfaces.HttpInterface;
import org.auth.csd.datalab.common.interfaces.MovementInterface;
import org.auth.csd.datalab.common.interfaces.PlacementInterface;
import org.auth.csd.datalab.common.models.Restarts;
import org.auth.csd.datalab.common.models.keys.*;
import org.auth.csd.datalab.common.models.values.MetaMetric;
import org.auth.csd.datalab.common.models.values.Metric;
import org.auth.csd.datalab.common.models.values.TimedMetric;
import org.auth.csd.datalab.services.*;

import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.auth.csd.datalab.common.Helpers.readEnvVariable;

/**
 * A new Data Node will be started in a separate JVM process when this class gets executed.
 */
public class ServerNodeStartup {

    //Get env variables
    public static final boolean CLUSTER_HEAD = readEnvVariable("CLUSTER_HEAD") != null && Boolean.parseBoolean(readEnvVariable("CLUSTER_HEAD"));
    public static final boolean APP_CACHE = readEnvVariable("APP_CACHE") != null && Boolean.parseBoolean(readEnvVariable("APP_CACHE"));
    public static final boolean PLACEMENT = readEnvVariable("PLACEMENT") == null || Boolean.parseBoolean(readEnvVariable("PLACEMENT"));
    //Monitoring caches
    public static final String LATEST_CACHE_NAME = "LatestMonitoring";
    public static final String HISTORICAL_CACHE_NAME = "HistoricalMonitoring";
    public static final String META_CACHE_NAME = "MetaMonitoring";
    //Analytics cache
    public static final String ANALYTICS_CACHE_NAME = "Analytics";
    //Replicas monitoring caches
    public static final String REPLICA_HOST_CACHE = "Replicas";
    public static final String RESTART_CACHE = "Restarts";
    //Optional application cache
    public static final String APP_CACHE_NAME = "ApplicationData";
    //Local consistent id
    public static String localNode = null;
    private static int evictionHours = 168;
    private static long totalSize = 512;
    private static final String DEFAULT_REGION_NAME = "Default_Region";
    private static final String PERSISTENCE_REGION_NAME = "Persistent_Region";

    private ServerNodeStartup() {
        throw new IllegalStateException("Utility class");
    }

    public static void createServer(String discovery, String hostname) throws IgniteException {
        localNode = hostname;
        Ignite ignite = Ignition.getOrStart(igniteConfiguration(discovery, hostname));
        ignite.cluster().state(ClusterState.ACTIVE);
        ignite.cluster().baselineAutoAdjustEnabled(true);
        ignite.cluster().baselineAutoAdjustTimeout(10000);
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
        myAtrr.put("data.head", CLUSTER_HEAD);
        //Create context
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setPeerClassLoadingEnabled(true);
        cfg.setUserAttributes(myAtrr);
        cfg.setLocalHost(hostname);
        cfg.setDiscoverySpi(new TcpDiscoverySpi()
                .setIpFinder(new TcpDiscoveryVmIpFinder().setAddresses(Arrays.asList(discovery.split(NodeStartup.DISCOVERY_DELIMITER))))
        );
        cfg.setConsistentId(hostname);
//        cfg.setIgniteInstanceName(hostname);
        //Enable events
        cfg.setIncludeEventTypes(EventType.EVT_NODE_JOINED, EventType.EVT_NODE_LEFT,
                EventType.EVT_NODE_FAILED);
        //Add the data regions to context
        cfg.setDataStorageConfiguration(dataStorageConfiguration());
        if(CLUSTER_HEAD) {
            //Cache configurations
            //Latest monitoring data cache
            CacheConfiguration<HostMetricKey, TimedMetric> latestCfg = new CacheConfiguration<>(LATEST_CACHE_NAME);
            latestCfg.setCacheMode(CacheMode.LOCAL)
                    .setIndexedTypes(HostMetricKey.class, TimedMetric.class)
                    .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.HOURS, evictionHours)))
                    .setEagerTtl(true);
            //Historical monitoring data cache
            CacheConfiguration<HostMetricTimeKey, Metric> historicalCfg = new CacheConfiguration<>(HISTORICAL_CACHE_NAME);
            historicalCfg.setCacheMode(CacheMode.LOCAL)
                    .setIndexedTypes(HostMetricTimeKey.class, Metric.class)
                    .setDataRegionName(PERSISTENCE_REGION_NAME)
                    .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                    .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.HOURS, evictionHours)))
                    .setTopologyValidator((TopologyValidator) collection -> true)
                    .setEagerTtl(true);
            //Metadata cache (metric meta)
            //Create near cache for local reads
            NearCacheConfiguration<HostMetricKey, MetaMetric> nearCfg = new NearCacheConfiguration<>();
            nearCfg.setNearStartSize(50 * 1024 * 1024);
            //Create meta cache with near configs
            CacheConfiguration<HostMetricKey, MetaMetric> metaCfg = new CacheConfiguration<>(META_CACHE_NAME);
            metaCfg.setCacheMode(CacheMode.REPLICATED)
                    .setIndexedTypes(HostMetricKey.class, MetaMetric.class)
                    .setNearConfiguration(nearCfg)
                    .setDataRegionName(PERSISTENCE_REGION_NAME);
            //Create replicas cache
            CacheConfiguration<HostMetricKey, Set<String>> replicaCfg = new CacheConfiguration<>(REPLICA_HOST_CACHE);
            replicaCfg.setCacheMode(CacheMode.REPLICATED)
                    .setIndexedTypes(HostMetricKey.class, Set.class)
                    .setDataRegionName(PERSISTENCE_REGION_NAME);
            //Create restarts cache
            CacheConfiguration<String, Restarts> restartCfg = new CacheConfiguration<>(RESTART_CACHE);
            restartCfg.setCacheMode(CacheMode.REPLICATED)
                    .setIndexedTypes(String.class, Restarts.class)
                    .setDataRegionName(PERSISTENCE_REGION_NAME);
            //Analytics cache
            CacheConfiguration<AnalyticKey, Metric> analyticsCfg = new CacheConfiguration<>(ANALYTICS_CACHE_NAME);
            analyticsCfg.setCacheMode(CacheMode.LOCAL)
                    .setIndexedTypes(AnalyticKey.class, Metric.class)
                    .setDataRegionName(PERSISTENCE_REGION_NAME)
                    .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.HOURS, evictionHours)))
                    .setEagerTtl(true);
            //Optional application k-v cache
            if (APP_CACHE) {
                cfg.setCacheConfiguration(latestCfg, historicalCfg, metaCfg, replicaCfg, analyticsCfg, restartCfg,
                        new CacheConfiguration<AnalyticKey, Metric>(APP_CACHE_NAME)
                                .setCacheMode(CacheMode.LOCAL)
                                .setIndexedTypes(AnalyticKey.class, Metric.class)
                                .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.HOURS, evictionHours)))
                                .setEagerTtl(true)
                );
            } else {
                cfg.setCacheConfiguration(latestCfg, historicalCfg, metaCfg, replicaCfg, restartCfg, analyticsCfg);
            }
        }
        //Activate services on nodes
        if(PLACEMENT) cfg.setServiceConfiguration(httpServiceConfiguration(), dataMngmServiceConfiguration(), movementServiceConfiguration(), placementServiceConfiguration());
        else cfg.setServiceConfiguration(httpServiceConfiguration(), dataMngmServiceConfiguration(), movementServiceConfiguration());
        return cfg;
    }

    private static ServiceConfiguration httpServiceConfiguration() {
        //Gives back a Node Singleton Service
        ServiceConfiguration sCfg = new ServiceConfiguration();
        sCfg.setName(HttpInterface.SERVICE_NAME);
        sCfg.setMaxPerNodeCount(1);
        sCfg.setNodeFilter(new DataFilter());
        sCfg.setService(new HttpService());
        return sCfg;
    }

    private static ServiceConfiguration dataMngmServiceConfiguration() {
        //Gives back a Node Singleton Service
        ServiceConfiguration sCfg = new ServiceConfiguration();
        sCfg.setName(DataManagementInterface.SERVICE_NAME);
        sCfg.setMaxPerNodeCount(1);
        sCfg.setNodeFilter(new DataFilter());
        sCfg.setService(new DataManagement());
        return sCfg;
    }

    private static ServiceConfiguration movementServiceConfiguration() {
        //Gives back a Node Singleton Service
        ServiceConfiguration sCfg = new ServiceConfiguration();
        sCfg.setName(MovementInterface.SERVICE_NAME);
        sCfg.setMaxPerNodeCount(1);
        sCfg.setNodeFilter(new DataFilter());
        sCfg.setService(new MovementService());
        return sCfg;
    }

    private static ServiceConfiguration placementServiceConfiguration() {
        //Gives back a Node Singleton Service
        ServiceConfiguration sCfg = new ServiceConfiguration();
        sCfg.setName(PlacementInterface.SERVICE_NAME);
        sCfg.setMaxPerNodeCount(1);
        sCfg.setNodeFilter(new HeadFilter());
        sCfg.setService(new PlacementService());
        return sCfg;
    }

    private static DataStorageConfiguration dataStorageConfiguration() {
        //Split size between the 2 regions
        long regionSize = Math.max(totalSize / 2, 100);
        //Set data regions
        DataStorageConfiguration dsc = new DataStorageConfiguration();
        dsc.setWalMode(WALMode.FSYNC);
        //Default in-memory region
        DataRegionConfiguration defaultRegion = new DataRegionConfiguration();
        defaultRegion.setName(DEFAULT_REGION_NAME);
        defaultRegion.setInitialSize(100L * 1024 * 1024); //100 mb
        defaultRegion.setMaxSize(regionSize * 1024 * 1024); //default is 256 mb
        defaultRegion.setPageEvictionMode(DataPageEvictionMode.RANDOM_2_LRU);
        defaultRegion.setMetricsEnabled(true);
        dsc.setDefaultDataRegionConfiguration(defaultRegion);
        //Persistence region
        DataRegionConfiguration regionWithPersistence = new DataRegionConfiguration();
        regionWithPersistence.setName(PERSISTENCE_REGION_NAME);
        regionWithPersistence.setInitialSize(100L * 1024 * 1024); //100 mb
        regionWithPersistence.setMaxSize(regionSize * 1024 * 1024); //default is 256 mb
        regionWithPersistence.setPersistenceEnabled(true);
        regionWithPersistence.setMetricsEnabled(true);
        dsc.setDataRegionConfigurations(regionWithPersistence);
        return dsc;
    }

}
