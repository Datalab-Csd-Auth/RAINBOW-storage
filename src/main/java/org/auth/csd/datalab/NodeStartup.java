package org.auth.csd.datalab;

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

    public static void main(String[] args) throws IgniteException, UnknownHostException {
        //Get instance type
        String instance = (readEnvVariable("NODE") != null && Objects.equals(readEnvVariable("NODE"), "CLIENT")) ? "CLIENT" : "SERVER";
        //Get discovery servers
        String discovery = (readEnvVariable("DISCOVERY") != null) ? readEnvVariable("DISCOVERY") : "localhost";
        //Get hostname
        String hostname = InetAddress.getLocalHost().getHostName();
        System.out.println(discovery);
        System.out.println(hostname);
        if(instance.equals("CLIENT")) ClientNodeStartup.createClient(discovery, hostname);
        else ServerNodeStartup.createServer(discovery, hostname);
    }


}
