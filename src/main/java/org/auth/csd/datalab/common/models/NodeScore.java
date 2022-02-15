package org.auth.csd.datalab.common.models;

import java.util.Objects;

public class NodeScore {
    public double score;
    public String node;

    public NodeScore(String node, double score) {
        this.node = node;
        this.score = score;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeScore nodeScore = (NodeScore) o;
        return Double.compare(nodeScore.score, score) == 0 && node.equals(nodeScore.node);
    }

    @Override
    public int hashCode() {
        return Objects.hash(score, node);
    }
}
