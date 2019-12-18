package com.huawei.bigdata.kafka.examples;


import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

public class SimplePartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        int partition = 0;
        try {
            // 指定分区逻辑，也就是key
            List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
            int numPartitions = partitions.size();
            partition = Integer.valueOf((int)key) % numPartitions;
        } catch (NumberFormatException ne) {
            // 如果解析失败，都分配到0分区上
            partition = 0;
        }
        return partition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}

