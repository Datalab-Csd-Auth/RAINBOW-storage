package org.auth.csd.datalab;

import org.apache.ignite.*;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.*;
import org.apache.ignite.logger.log4j.Log4JLogger;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.metric.log.LogExporterSpi;
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
    private static final String appRegionName = "App_Region";

    public static void createServer(String discovery, String hostname) throws IgniteException {
        Ignite ignite = Ignition.start(igniteConfiguration(discovery, hostname));
        ignite.cluster().state(ClusterState.ACTIVE);
        ignite.cluster().baselineAutoAdjustEnabled(true);
        ignite.cluster().baselineAutoAdjustTimeout(10);
        System.out.println(ignite.cluster().localNode().id());
    }

    private static IgniteConfiguration igniteConfiguration(String discovery, String hostname) {
        //Set cluster identification and custom parameters
        //System.setProperty("java.net.preferIPv4Stack", "true"); //Only use ipv4
        //Get eviction rate
        String tmpEvict = readEnvVariable("EVICTION");
        if (tmpEvict != null) evictionHours = Integer.parseInt(tmpEvict);
        //Get total size of data regions
        String tmpSize = readEnvVariable("SIZE");
        if (tmpSize != null) totalSize = Long.parseLong(tmpSize);
        //Split size between the 2 regions
        long regionSize = Math.max(totalSize / 2, 100);
        //Set attributes
        Map<String, Boolean> myAtrr = new HashMap<>();
        myAtrr.put("data.node", true);
        //Create context
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setPeerClassLoadingEnabled(true);
        cfg.setUserAttributes(myAtrr);
        cfg.setLocalHost(hostname);
        cfg.setDiscoverySpi(new TcpDiscoverySpi()
                .setIpFinder(new TcpDiscoveryVmIpFinder().setAddresses(Arrays.asList(discovery.split(NodeStartup.discoveryDelimiter))))
        );
        //Set data regions
        DataStorageConfiguration dsc = new DataStorageConfiguration();
        //Default region
        DataRegionConfiguration defaultRegion = new DataRegionConfiguration();
        defaultRegion.setName(defaultRegionName);
        defaultRegion.setInitialSize(100 * 1024 * 1024);
        defaultRegion.setMaxSize(regionSize * 1024 * 1024);
        defaultRegion.setPageEvictionMode(DataPageEvictionMode.RANDOM_2_LRU);
        defaultRegion.setMetricsEnabled(true);
        dsc.setDefaultDataRegionConfiguration(defaultRegion);
        //Persistence region
        DataRegionConfiguration regionWithPersistence = new DataRegionConfiguration();
        regionWithPersistence.setName(persistenceRegionName);
        regionWithPersistence.setInitialSize(100 * 1024 * 1024);
        regionWithPersistence.setMaxSize(regionSize * 1024 * 1024);
        regionWithPersistence.setPersistenceEnabled(true);
        regionWithPersistence.setMetricsEnabled(true);
        dsc.setDataRegionConfigurations(regionWithPersistence);
        //Add to context
        cfg.setDataStorageConfiguration(dsc);

        //Cache configurations
        //Latest monitoring data cache
        CacheConfiguration<String, TimedMetric> latestCfg = new CacheConfiguration<>(latestCacheName);
        latestCfg.setCacheMode(CacheMode.LOCAL)
                .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.HOURS, evictionHours)))
                .setEagerTtl(true);
        //Metadata cache (metric meta)
        CacheConfiguration<MetaMetricKey, MetaMetric> metaCfg = new CacheConfiguration<>(metaCacheName);
        metaCfg.setCacheMode(CacheMode.LOCAL)
                .setIndexedTypes(MetaMetricKey.class, MetaMetric.class)
                .setDataRegionName(persistenceRegionName);
        //Historical monitoring data cache
        CacheConfiguration<MetricKey, Metric> historicalCfg = new CacheConfiguration<>(historicalCacheName);
        historicalCfg.setCacheMode(CacheMode.LOCAL)
                .setIndexedTypes(MetricKey.class, Metric.class)
                .setDataRegionName(persistenceRegionName);
        //Analytics cache
        CacheConfiguration<String, TimedMetric> analyticsCfg = new CacheConfiguration<>(analyticsCacheName);
        analyticsCfg.setCacheMode(CacheMode.LOCAL);
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
        cfg.setServiceConfiguration(serviceConfiguration());

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
