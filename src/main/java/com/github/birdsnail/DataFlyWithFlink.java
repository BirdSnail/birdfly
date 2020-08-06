package com.github.birdsnail;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.birdsnail.pojo.HBJJZInfo;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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
//		socketStream.map(s -> {
//			ObjectMapper objectMapper = new ObjectMapper();
//			return objectMapper.readValue(s, HBJJZInfo.class);
//		}).print();
		socketStream.print();

		env.execute("Kafka数据入库");
	}
}
