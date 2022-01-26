package org.auth.csd.datalab;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;

import static org.auth.csd.datalab.common.Helpers.readEnvVariable;

/**
 * A new Data Node will be started in a separate JVM process when this class gets executed.
 */
public class NodeStartup {

    public static final String discoveryDelimiter = ",";

    public static void main(String[] args) throws IgniteException, UnknownHostException, IgniteCheckedException {
        //Get hostname
        String hostname = (readEnvVariable("HOSTNAME") != null) ? readEnvVariable("HOSTNAME") : InetAddress.getLocalHost().getHostName();
        //Get discovery servers
        String discovery = (readEnvVariable("DISCOVERY") != null) ? readEnvVariable("DISCOVERY") : hostname;
        System.out.println("Discovery servers: " + discovery);
        System.out.println("Hostname: " + hostname);
        ServerNodeStartup.createServer(discovery, hostname);
    }


}
