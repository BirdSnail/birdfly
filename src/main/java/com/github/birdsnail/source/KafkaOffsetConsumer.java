package com.github.birdsnail.source;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * 获取kafka中topic每个分区的offset
 *
 * @author BirdSnail
 * @date 2020/8/18
 */
public class KafkaOffsetConsumer {

	public static final String TOPIC = "quickstart-events";
	public static final String CONSUMER_GROUP = "lookupKafkaOffset";

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.152.24:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

		Consumer<String, String> consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
		List<PartitionInfo> partitionInfos = consumer.partitionsFor(TOPIC);
		Collection<TopicPartition> topicPartitions = partitionInfos.stream()
				.map(p -> new TopicPartition(p.topic(), p.partition()))
				.collect(Collectors.toList());

		consumer.assign(topicPartitions);
//		consumer.seekToEnd(topicPartitions);

		AdminClient kafkaClient = AdminClient.create(props);

		while (true) {
			Map<TopicPartition, Long> partitionOffset = consumer.endOffsets(topicPartitions);
			print(partitionOffset);

			ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
			for (ConsumerRecord<String, String> record : records) {
				System.out.println("value: " + record.value());
			}

			ListConsumerGroupOffsetsResult offsetsResult = kafkaClient.listConsumerGroupOffsets(CONSUMER_GROUP);

			for (TopicPartition partition : topicPartitions) {
				OffsetAndMetadata offsetAndMetadata = offsetsResult.partitionsToOffsetAndMetadata().get().get(partition);
				System.out.println("current offset: " + offsetAndMetadata.offset());

				System.out.println(
						String.format("topic:%s | partition:%s-->next offset:[%s]",
								partition.topic(), partition.partition(), consumer.position(partition)));
			}
			Thread.sleep(2000L);
		}
	}


	private static void print(Map<TopicPartition, Long> partitionOffset) {
		partitionOffset.forEach((partition, offset) -> {
			System.out.println(String.format(
					"topic:%s | partition:%s-->partition max offset[%s]", partition.topic(), partition.partition(), offset));
		});
	}
}
