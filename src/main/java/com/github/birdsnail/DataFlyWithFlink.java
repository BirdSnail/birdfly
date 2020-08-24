package com.github.birdsnail;

import com.github.birdsnail.pojo.HBJJZInfo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;


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

	public static final String INSERT = "insert into collect_result_minute(size) VALUES(?)";

	public static void main(String[] args) throws Exception {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "snail01:9092");
		properties.setProperty("group.id", "kafka-hbjjz");

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStateBackend(new FsStateBackend("hdfs://192.168.152.57:9000/flink/checkpoints"));
		env.enableCheckpointing(1000 * 2 * 60, CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10 * 1000);
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);

		DataStream<HBJJZInfo> stream = env
				.addSource(new FlinkKafkaConsumer<>("hbjjz", new SimpleStringSchema(), properties))
				.setParallelism(3)
				.uid("kafka_source")
				.map(jjz -> {
					System.out.println(jjz);
					ObjectMapper objectMapper = new ObjectMapper();
					objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
					return objectMapper.readValue(jjz, HBJJZInfo.class);
				})
				.uid("jjz_map");

		// 数据备份的流
		stream
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

						JdbcExecutionOptions.builder()
								.withBatchSize(2000)
								.withBatchIntervalMs(30 * 1000)
								.withMaxRetries(1)
								.build(),

						createJdbcConnectionInfo()))
				.setParallelism(1)
				.uid("jdbc_sink");

		// 每小时数据统计
		stream.timeWindowAll(Time.seconds(60L))
				.aggregate(new CountAggregateFunction())
				.setParallelism(1)
				.uid("count_aggregate")
				.addSink(JdbcSink.sink(
						INSERT,
						(ps, t) -> ps.setInt(1, t),
						JdbcExecutionOptions.builder()
								.withBatchSize(1)
								.withMaxRetries(1)
								.build(),
						createJdbcConnectionInfo()
				))
				.uid("jdbc_sink_summary_minute");
		env.execute("Kafka数据入库");
	}

	private static JdbcConnectionOptions createJdbcConnectionInfo() {
		return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
				.withUrl("jdbc:mysql://192.168.152.24:3306/flink_kafka")
				.withDriverName("com.mysql.cj.jdbc.Driver")
				.withUsername("root")
				.withPassword("root")
				.build();
	}


	private static class CountAggregateFunction implements AggregateFunction<HBJJZInfo, Integer, Integer> {

		@Override
		public Integer createAccumulator() {
			return 0;
		}

		@Override
		public Integer add(HBJJZInfo value, Integer accumulator) {
			return ++accumulator;
		}

		@Override
		public Integer getResult(Integer accumulator) {
			return accumulator;
		}

		@Override
		public Integer merge(Integer a, Integer b) {
			return a + b;
		}
	}

}
