package com.learning.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {

	public static void main(String[] args) {
		
		Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
		
		Properties prop = new Properties();
		prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
		
		for (int i=0; i<10; i++) 
		{
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("first-topic", "Message-" + i + " from Java Code");	
			producer.send(record, new Callback() {
				
				@Override
				public void onCompletion(RecordMetadata metadata, Exception ex) {
					if (ex != null) {
						logger.error("Something went wrong while consuming messages - " + ex.getMessage());
					} else {
						logger.info("Received Metadata: \n" +
								"Topic : " + metadata.topic() + 
								" -- Partition : " + metadata.partition() + 
								" -- Offset : " + metadata.offset() + 
								" -- Timestamp : " + metadata.timestamp());
					}
				}
			});
		}
		
		producer.flush();		
		producer.close();
	}

}
