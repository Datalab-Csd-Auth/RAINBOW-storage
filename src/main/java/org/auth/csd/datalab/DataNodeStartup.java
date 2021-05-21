package org.auth.csd.datalab;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterState;

/**
 * A new Data Node will be started in a separate JVM process when this class gets executed.
 */
public class DataNodeStartup {

    public static void main(String[] args) throws IgniteException {
        Ignite ignite = Ignition.start("/opt/assets/config/ignite-server.xml");
        ignite.cluster().state(ClusterState.ACTIVE);
        ignite.cluster().baselineAutoAdjustEnabled(true);
        ignite.cluster().baselineAutoAdjustTimeout(30000);

        System.out.println(ignite.cluster().localNode().id());

    }

}