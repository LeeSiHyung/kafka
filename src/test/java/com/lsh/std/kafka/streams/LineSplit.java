package com.lsh.std.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class LineSplit {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-linesplit");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String());

        // 토폴리지를 정의할 빌더
        final StreamsBuilder builder = new StreamsBuilder();

        // streams-plaintext-input 토픽으로부터 새로운 입력 스트림을 생성하고, 이것을 다시 streams-pipe-output 토픽으로 전달하도록 만듬
        // KStream<String, String> source = builder.stream("streams-plaintext-input");
        // KStream<String, String> words = source.flatMapValues(new ValueMapper<String, Iterable<? extends String>>() {
        //     @Override
        //     public Iterable<? extends String> apply(String value) {
        //         return Arrays.asList(value.split("\\W+"));
        //     }
        // });
        // source.to("streams-pipe-output");

        KStream<String, String> source = builder.stream("streams-plaintext-input");
        source.flatMapValues(value -> Arrays.asList(value.split("\\W|")))
                .to("streams-linesplit-output");

        // 최종적인 토폴리지를 만들기 위해 빌더에서 build 실행
        final Topology topology = builder.build();
        System.out.println(topology.describe());

        // 스트림즈 객체
        final KafkaStreams streams = new KafkaStreams(topology, props);

        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook"){
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e){
            System.exit(1);
        }
        System.exit(0);
    }
}
