package com.learning.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithAssignSeek {

	public static void main(String[] args) {
		Logger logger = LoggerFactory.getLogger(ConsumerDemoWithAssignSeek.class);

		Properties prop = new Properties();
		prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);
		
		TopicPartition partition = new TopicPartition("first-topic", 0);
		long offset = 15L;
		
		consumer.assign(Arrays.asList(partition));
		consumer.seek(partition, offset);
		
		boolean keepOnReading = true;
		int msgToRead = 10;
		int msgReadSoFar = 0;

		while (keepOnReading) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));

			for (ConsumerRecord<String, String> record : records) {
				logger.info("Receiving msg : \n" + "Topic : " + record.topic() + " -- Partition : " + record.partition()
						+ " -- Key : " + record.key() + " -- Value : " + record.value());
				
				msgReadSoFar++;
				
				if (msgReadSoFar > msgToRead) {
					keepOnReading = false;
					break;
				}
			}
		}
		
		consumer.close();
	}

}
