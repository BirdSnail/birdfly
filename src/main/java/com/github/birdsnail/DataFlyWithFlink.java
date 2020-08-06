package com.github.birdsnail;

import com.github.birdsnail.pojo.HBJJZInfo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * Flink消费kafka数据实现数据入库
 *
 * @author BirdSnail
 * @date 2020/8/6
 */
public class DataFlyWithFlink {

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("topic", new SimpleStringSchema(), properties));

        DataStream<String> socketStream = env.socketTextStream("localhost", 9986);
        socketStream
                .map(jjz -> {
                    System.out.println(jjz);
                    ObjectMapper objectMapper = new ObjectMapper();
                    objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
//			        objectMapper.registerModule(new JavaTimeModule());
                    HBJJZInfo value = objectMapper.readValue(jjz, HBJJZInfo.class);
                    return value.getName();
                })

                .print();


        env.execute("Kafka数据入库");
    }
}
