package org.auth.csd.datalab.common.models;

import org.auth.csd.datalab.common.models.values.MetaMetric;
import org.auth.csd.datalab.common.models.values.TimedMetric;

import java.util.List;

public class Monitoring {
    public MetaMetric metadata;
    public List<TimedMetric> values;

    public Monitoring(MetaMetric metadata, List<TimedMetric> values) {
        this.metadata = metadata;
        this.values = values;
    }
}
