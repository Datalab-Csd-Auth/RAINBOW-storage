package org.auth.csd.datalab;

import org.apache.ignite.IgniteException;

import java.net.UnknownHostException;
import java.util.Objects;

import static org.auth.csd.datalab.common.Helpers.readEnvVariable;

/**
 * A new Data Node will be started in a separate JVM process when this class gets executed.
 */
public class NodeStartup {

    public static void main(String[] args) throws IgniteException, UnknownHostException {
        String instance = (readEnvVariable("NODE") != null && Objects.equals(readEnvVariable("NODE"), "CLIENT")) ? "CLIENT" : "SERVER";
        if(instance.equals("CLIENT")) ClientNodeStartup.createClient();
        else ServerNodeStartup.createServer();
    }


}
