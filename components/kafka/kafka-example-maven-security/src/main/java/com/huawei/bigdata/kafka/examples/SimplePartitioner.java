package com.huawei.bigdata.kafka.examples;

import kafka.producer.Partitioner;
import kafka.utils.*;

public class SimplePartitioner implements Partitioner
{
    public SimplePartitioner(VerifiableProperties props)
    {
    }

    public int partition(Object key, int a_numPartitions)
    {
        int partition = 0;
        String partitionKey = (String) key;

        try
        {
            // 指定分区逻辑，也就是key
            partition = Integer.parseInt(partitionKey) % a_numPartitions;
        }
        catch (NumberFormatException ne)
        {
            // 如果解析失败，都分配到0分区上
            partition = 0;
        }

        return partition;
    }
}
