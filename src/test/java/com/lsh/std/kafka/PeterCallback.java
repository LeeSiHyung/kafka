package com.lsh.std.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class PeterCallback implements Callback {
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if(metadata != null){
            System.out.println("Partition : " + metadata.partition() + ", Offset : " + metadata.offset());
        }
        else{
            exception.printStackTrace();
        }
    }
}
