package com.huawei.storm.example.kafka;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.kafka.spout.KafkaSpoutTupleBuilder;

import org.apache.storm.tuple.Values;

/**
 * 从consumerRecord中构造一条 Tuple
 */
public class TopicsTupleBuilder<K, V> extends KafkaSpoutTupleBuilder<K,V>{

    private static final long serialVersionUID = -2521108716808001532L;

    public TopicsTupleBuilder(String... topics) {
        super(topics);
    }
    
    @Override
    public List<Object> buildTuple(ConsumerRecord<K, V> consumerRecord) {
        return new Values(consumerRecord.value(),
                consumerRecord.topic(),
                consumerRecord.partition(),
                consumerRecord.offset(),
                consumerRecord.key());
    }

}
