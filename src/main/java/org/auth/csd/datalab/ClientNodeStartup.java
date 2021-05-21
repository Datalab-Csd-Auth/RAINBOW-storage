package org.auth.csd.datalab;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.auth.csd.datalab.common.filter.ClientFilter;
import org.auth.csd.datalab.services.ClientExtractionService;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.auth.csd.datalab.common.Helpers.readEnvVariable;
import static org.auth.csd.datalab.common.interfaces.ClientExtractionInterface.SERVICE_NAME;

/**
 * A new Data Node will be started in a separate JVM process when this class gets executed.
 */
public class ClientNodeStartup {

    public static void createClient() throws IgniteException, UnknownHostException {

        String discovery = (readEnvVariable("DISCOVERY") != null) ? readEnvVariable("DISCOVERY") : "localhost";
        String hostname = InetAddress.getLocalHost().getHostName();
        System.out.println(discovery);
        System.out.println(hostname);

        Ignite ignite = Ignition.start(igniteConfiguration(discovery, hostname));
        System.out.println(ignite.cluster().localNode().id());

    }

    private static IgniteConfiguration igniteConfiguration(String discovery,String hostname) {
        Map<String, Boolean> myAtrr = new HashMap<>();
        myAtrr.put("data.node", false);
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setPeerClassLoadingEnabled(true);
        cfg.setUserAttributes(myAtrr);
        cfg.setClientMode(true);
        cfg.setLocalHost(hostname);
        cfg.setDiscoverySpi(new TcpDiscoverySpi()
                .setIpFinder(new TcpDiscoveryVmIpFinder().setAddresses(Arrays.asList(discovery.split(","))))
        );

        cfg.setServiceConfiguration(serviceConfiguration());

        return cfg;
    }

    private static ServiceConfiguration serviceConfiguration() {
        // Gives back a Node Singleton Service
        ServiceConfiguration cfg = new ServiceConfiguration();
        cfg.setName(SERVICE_NAME);
        cfg.setMaxPerNodeCount(1);
        cfg.setNodeFilter(new ClientFilter());
        cfg.setService(new ClientExtractionService());
        return cfg;
    }

}
