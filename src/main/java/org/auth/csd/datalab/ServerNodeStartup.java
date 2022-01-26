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
import org.auth.csd.datalab.common.models.keys.AnalyticKey;
import org.auth.csd.datalab.common.models.keys.MetricKey;
import org.auth.csd.datalab.common.models.keys.MetricTimeKey;
import org.auth.csd.datalab.common.models.values.MetaMetric;
import org.auth.csd.datalab.common.models.values.Metric;
import org.auth.csd.datalab.common.models.values.TimedMetric;
import org.auth.csd.datalab.services.DataService;
import org.auth.csd.datalab.services.RebalanceService;

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

    public static final Boolean cluster_head = readEnvVariable("CLUSTER_HEAD") != null && Boolean.parseBoolean(readEnvVariable("CLUSTER_HEAD"));
    public static final Boolean app_cache = readEnvVariable("APP_CACHE") != null && Boolean.parseBoolean(readEnvVariable("APP_CACHE"));
    public static final String latestCacheName = "LatestMonitoring";
    public static final String historicalCacheName = "HistoricalMonitoring";
    public static final String metaCacheName = "MetaMonitoring";
    public static final String appCacheName = "ApplicationData";
    public static final String analyticsCacheName = "Analytics";
    private static int evictionHours = 168;
    private static long totalSize = 512;
    private static final String defaultRegionName = "Default_Region";
    private static final String persistenceRegionName = "Persistent_Region";
    public static void createServer(String discovery, String hostname) throws IgniteException {
        Ignite ignite = Ignition.start(igniteConfiguration(discovery, hostname));
        ignite.cluster().state(ClusterState.ACTIVE);
        ignite.cluster().baselineAutoAdjustEnabled(true);
        ignite.cluster().baselineAutoAdjustTimeout(60000);
        System.out.println("Local node id: " + ignite.cluster().localNode().consistentId());
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
        CacheConfiguration<MetricKey, TimedMetric> latestCfg = new CacheConfiguration<>(latestCacheName);
        latestCfg.setCacheMode(CacheMode.LOCAL)
                .setIndexedTypes(MetricKey.class, TimedMetric.class)
                .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.HOURS, evictionHours)))
                .setEagerTtl(true);
        //Metadata cache (metric meta)
        CacheConfiguration<MetricKey, MetaMetric> metaCfg = new CacheConfiguration<>(metaCacheName);
        metaCfg.setCacheMode(CacheMode.LOCAL)
                .setIndexedTypes(MetricKey.class, MetaMetric.class)
                .setDataRegionName(persistenceRegionName)
                .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.HOURS, evictionHours)))
                .setEagerTtl(true);
        //Historical monitoring data cache
        CacheConfiguration<MetricTimeKey, Metric> historicalCfg = new CacheConfiguration<>(historicalCacheName);
        historicalCfg.setCacheMode(CacheMode.LOCAL)
                .setIndexedTypes(MetricTimeKey.class, Metric.class)
                .setDataRegionName(persistenceRegionName)
                .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.HOURS, evictionHours)))
                .setEagerTtl(true);
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
                    new CacheConfiguration<>(appCacheName)
                            .setCacheMode(CacheMode.LOCAL)
                            .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.HOURS, evictionHours)))
                            .setEagerTtl(true)
            );
        } else {
            cfg.setCacheConfiguration(latestCfg, metaCfg, historicalCfg, analyticsCfg);
        }
        //Activate services on nodes
        cfg.setServiceConfiguration(dataServiceConfiguration(), rebalanceServiceConfiguration());

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

    private static ServiceConfiguration rebalanceServiceConfiguration() {
        //Gives back a Node Singleton Service
        ServiceConfiguration sCfg = new ServiceConfiguration();
        sCfg.setName(RebalanceInterface.SERVICE_NAME);
        sCfg.setMaxPerNodeCount(1);
        sCfg.setNodeFilter(new HeadFilter());
        sCfg.setService(new RebalanceService());
        return sCfg;
    }

    private static ServiceConfiguration dataServiceConfiguration() {
        //Gives back a Node Singleton Service
        ServiceConfiguration sCfg = new ServiceConfiguration();
        sCfg.setName(DataService.SERVICE_NAME);
        sCfg.setMaxPerNodeCount(1);
        sCfg.setNodeFilter(new DataFilter());
        sCfg.setService(new DataService());
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
