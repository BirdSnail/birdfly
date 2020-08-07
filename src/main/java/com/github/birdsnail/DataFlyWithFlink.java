package com.github.birdsnail;

import com.github.birdsnail.pojo.HBJJZInfo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.table.JdbcUpsertTableSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Properties;

/**
 * Flink消费kafka数据实现数据入库
 *
 * @author BirdSnail
 * @date 2020/8/6
 */
public class DataFlyWithFlink {

	public static final String INSERT_OR_UPDATE = "insert into hbjjz(id, name, city, blDate, validDateTime, num) VALUES (?,?,?,?,?,?)" +
			"ON DUPLICATE KEY UPDATE id=VALUES(id),name=VALUES(name),city=VALUES(city), blDate=VALUES(blDate),validDateTime=VALUES(validDateTime),num=VALUES(num)";

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
					return objectMapper.readValue(jjz, HBJJZInfo.class);
				})

				.addSink(JdbcSink.sink(
						INSERT_OR_UPDATE,
						(ps, t) -> {
							ps.setInt(1, t.getId());
							ps.setString(2, t.getName());
							ps.setString(3, t.getCity());
							ps.setObject(4, t.getBlDate());
							ps.setObject(5, t.getValidDateTime());
							ps.setInt(6, t.getNum());
						},
						new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
								.withUrl("jdbc:mysql://192.168.152.24:3306/flink_kafka")
								.withDriverName("v")
								.withUsername("root")
								.withPassword("root")
								.build()
				));


		env.execute("Kafka数据入库");
	}
}
