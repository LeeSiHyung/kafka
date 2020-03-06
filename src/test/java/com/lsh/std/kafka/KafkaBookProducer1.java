package com.lsh.std.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class KafkaBookProducer1 {

    public static void main(String[] args) {
        Properties props = new Properties();
        // props.put("bootstrap.servers", "f7adc472a326:9092");
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        try{
            // producer.send(new ProducerRecord<String, String>("peter-topic", "Apache Kafka is distributed streaming platform"));
            RecordMetadata metadata = producer.send(
                    new ProducerRecord<String, String>("peter-topic", "Apache Kafka is distributed streaming platform")
            ).get();
            System.out.printf("Partition: %d, offset %d", metadata.partition(), metadata.offset());
        }
        catch (Exception e){
            e.printStackTrace();
        }
        finally {
            producer.close();
        }
    }
}
