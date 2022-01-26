package org.auth.csd.datalab.common.filter;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgnitePredicate;

public class HeadFilter implements IgnitePredicate<ClusterNode> {


    @Override
    public boolean apply(ClusterNode clusterNode) {
        return Boolean.TRUE.equals(clusterNode.attribute("data.head"));
    }
}
