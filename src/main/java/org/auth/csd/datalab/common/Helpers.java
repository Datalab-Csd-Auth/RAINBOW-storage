package org.auth.csd.datalab.common;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgnitePredicate;

import java.util.Set;

public class Helpers {

    /**
     * Method to read environment variables
     *
     * @param key The key of the variable
     * @return The variable
     */
    public static String readEnvVariable(String key) {
        if (System.getenv().containsKey(key)) {
            return System.getenv(key);
        } else return null;
    }

    public static IgnitePredicate<ClusterNode> getNodesByHostnames(Set<String> hostnames){
        IgnitePredicate<ClusterNode> filter;
        if (!hostnames.isEmpty()) filter = (key) -> hostnames.stream().distinct().anyMatch(key.hostNames()::contains);
        else filter = null;
        return filter;
    }

}
