package org.auth.csd.datalab.common.filter;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgnitePredicate;

public class ClientFilter implements IgnitePredicate<ClusterNode> {


    @Override
    public boolean apply(ClusterNode clusterNode) {
        return clusterNode.isClient();
    }
}
