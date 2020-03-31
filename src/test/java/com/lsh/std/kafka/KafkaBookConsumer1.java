package com.lsh.std.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaBookConsumer1 {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "");
        props.put("group.id", "");
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "latest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("peter-topic"));
        try{
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String,String> record : records)
                System.out.printf("Topic: %s, Partition: %s, Offset: %d, Key : %s, Value: %s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value());
        }
        finally {
            consumer.close();
        }

    }

}
