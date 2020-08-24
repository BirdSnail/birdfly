package com.github.birdsnail.source;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.github.birdsnail.pojo.HBJJZInfo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.Random;

/**
 * 向kafka中发送消息
 *
 * @author BirdSnail
 * @date 2020/8/13
 */
public class KafkaMessage {

	public static final String TOPIC = "hbjjz";
	private static final String[] names = {"张三", "李四", "王五"};
	private static String[] cities = {"wuhan", "shanghai", "beijing"};

	private static final Random random = new Random();

	public static ObjectMapper objectMapper = new ObjectMapper();

	public static void main(String[] args) throws InterruptedException {
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.152.24:9092");
		props.put("transactional.id", "my-transactional-id");
		Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());

		producer.initTransactions();

		try {
			while (true) {
				producer.beginTransaction();
				for (int i = 0; i < 10; i++) {
					producer.send(createKafkaRecord(TOPIC, random.nextInt(2000)));
				}
				// preCommit
				producer.flush();
				producer.commitTransaction();
				Thread.sleep(2000L);
			}
		} catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
			// We can't recover from these exceptions, so our only option is to close the producer and exit.
			producer.close();
		} catch (Exception e) {
			// For all other exceptions, just abort the transaction and try again.
			producer.abortTransaction();
			e.printStackTrace();
		}
		producer.close();
	}

	private static ProducerRecord<String, String> createKafkaRecord(String topic, int id) throws JsonProcessingException {
		int index = random.nextInt(names.length);
		int cityIndex = random.nextInt(cities.length);
		HBJJZInfo info = HBJJZInfo.builder()
				.id(id)
				.name(names[index])
				.blDate(LocalDate.now())
				.city(cities[cityIndex])
				.num(random.nextInt(1000))
				.validDateTime(LocalDateTime.now())
				.srcSystem("test-kafka")
				.build();

		System.out.println(info);

		return new ProducerRecord<>(topic, info.getCity(), objectMapper.writeValueAsString(info));
	}

}
