/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 * The class that will be used to determine the partition in which the record
 * will go. In the demo topic, there is only one partition, so I have commented
 * this property. You can create your custom partitioner by implementing the
 * CustomPartitioner interface.
 */
package com.mycompany.kafkatutorial;

import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

public class CustomPartitioner implements Partitioner {

    private static final int PARTITION_COUNT = 50;

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        Integer keyInt = Integer.parseInt(key.toString());
        return keyInt % PARTITION_COUNT;
    }

    @Override
    public void close() {
    }
}
