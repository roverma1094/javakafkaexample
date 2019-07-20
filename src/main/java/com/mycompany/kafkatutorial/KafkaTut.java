/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.kafkatutorial;

import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 *
 * @author SOFT3
 */
public class KafkaTut {

    public static void main(String[] args) {
        runProducer();
        runConsumer();
    }

    static void runConsumer() {
        System.out.println("*****************************KAFKA CONSUMER*****************************");
        Consumer<Long, String> consumer = ConsumerCreator.createConsumer();
        int noMessageFound = 0;
        while (true) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
            // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
            if (consumerRecords.count() == 0) {
                noMessageFound++;
                if (noMessageFound > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT) // If no message found count is reached to threshold exit loop.  
                {
                    break;
                } else {
                    continue;
                }
            }
            //print each record. 
            for (ConsumerRecord<Long, String> record : consumerRecords) {
                System.out.println("Record Key " + record.key());
                System.out.println("Record value " + record.value());
                System.out.println("Record partition " + record.partition());
                System.out.println("Record offset " + record.offset());
            }
//            consumerRecords.forEach(record -> {
//                System.out.println("Record Key " + record.key());
//                System.out.println("Record value " + record.value());
//                System.out.println("Record partition " + record.partition());
//                System.out.println("Record offset " + record.offset());
//            });
            // commits the offset of record to broker. 
            consumer.commitAsync();
        }
        consumer.close();
    }

    static void runProducer() {
        System.out.println("*****************************KAFKA PRODUCER*****************************");
        Producer<Long, String> producer = ProducerCreator.createProducer();
        for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
            ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(IKafkaConstants.TOPIC_NAME, (long) index, "This is record " + index);
            try {
                RecordMetadata metadata = producer.send(record).get();
                System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
                        + " with offset " + metadata.offset());
            } catch (ExecutionException e) {
                System.out.println("Error in sending record");
                System.out.println(e);
            } catch (InterruptedException e) {
                System.out.println("Error in sending record");
                System.out.println(e);
            }
        }
    }
}
