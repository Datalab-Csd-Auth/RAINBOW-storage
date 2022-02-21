package org.auth.csd.datalab.common;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgnitePredicate;

import javax.persistence.Tuple;
import java.util.List;
import java.util.Set;

public class Helpers {

    private Helpers() {
    }

    public static class Tuple2<K, V> {

        public K first;
        public V second;

        public Tuple2(K first, V second){
            this.first = first;
            this.second = second;
        }
    }

    public static Tuple2<Double, Long> combineTuples(Tuple2<Double, Long> tuple1, Tuple2<Double, Long> tuple2, int agg){
        switch (agg){
            case 0: {
                if (tuple1.first > tuple2.first) return tuple1;
                else return tuple2;
            }
            case 1: {
                if (tuple1.first < tuple2.first) return tuple1;
                else return tuple2;
            }
            case 2: {
                tuple1.first += tuple2.first;
                return tuple1;
            }
            case 3: {
                tuple1.first += tuple2.first;
                tuple1.second += tuple2.second;
                return tuple1;
            }
            default: return null;
        }
    }

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

    public static FieldsQueryCursor<List<?>> getQueryValues(IgniteCache cache, String sql) {
        SqlFieldsQuery query = new SqlFieldsQuery(sql);
        return cache.query(query);
    }

}
