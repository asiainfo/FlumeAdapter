package com.asiainfo.ocdp.flume.sink.kafka.plugin;
import kafka.producer.Partitioner;
/**
 * Created by peng on 2016/11/23.
 */
public class HashPartitioner implements Partitioner {
    @Override
    public int partition(Object key, int numPartitions) {
        int partition;
        String _key = (String)key;
        partition = Math.abs(_key.hashCode()) % numPartitions;
        return partition;
    }
}
