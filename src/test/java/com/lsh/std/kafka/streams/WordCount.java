package com.lsh.std.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WordCount {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String());

        // 토폴리지를 정의할 빌더
        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source = builder.stream("streams-plaintext-input");
        // source.flatMapValues(value -> Arrays.asList(value.split("\\W|"))).to("streams-linesplit-output");
        // source.flatMapValues(new ValueMapper<String, Iterable<?>>() {
        //     @Override
        //     public Iterable<?> apply(String value) {
        //         return Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+"));
        //     }
        // })
        source.flatMapValues(v -> {
            return Arrays.asList(v.toLowerCase(Locale.getDefault()).split("\\W+"));
        })
        // .groupBy((k, v) -> {return v;})

        // 같은 키를 가진 데이터를 모음. 새로운 키/값 쌍을 만들기 위해 KeyValueMapper를 사용한다.
        .groupBy(new KeyValueMapper<String, String, String>() {
            @Override
            public String apply(String key, String value) {
                return value;
            }
        })

        // 스트림을 테이블로 변환 count 입력 groupBy 결과인 String Long 인데 이를 Byte byte[]로 변환해서 count-store에 테이블 형식으로 저장
        // 여기에 저장된 값은 실시간 쿼리가 가능하다.
        .count(Materialized.<String,Long, KeyValueStore<Bytes, byte[]>>as("count-store"))
                // count의 결과로 생성된 counts-store 테이블 데이터를 스트림으로 변환
                .toStream()
                // 변환된 스트림을 아래 토픽에 저장, 저장할 때 데이터는 각각 Byte, byte[]에서 문자열과 롱타입으로 변환한다.
                .to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));

        source.flatMapValues(new ValueMapper<String, Iterable<?>>() {
            @Override
            public Iterable<?> apply(String value) {
                return Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+"));
            }
        })
        ;

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
