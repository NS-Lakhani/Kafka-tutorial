package com.learning.kafka.twitter;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducerDemo {

	public TwitterProducerDemo() {
		
	}

	public static void main(String[] args) {
		
		Logger logger = LoggerFactory.getLogger(TwitterProducerDemo.class);
		
		Properties prop = new Properties();
		prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
		Client client = new TwitterProducerDemo().run(msgQueue);
		client.connect();
		
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

		while (!client.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				logger.error(e.getMessage());
				client.stop();
			}
			
			if (msg != null) {
				logger.info(msg);				
				producer.send(new ProducerRecord<String, String>("twitter_tweets", msg), new Callback() {
					
					@Override
					public void onCompletion(RecordMetadata metadata, Exception ex) {
						if (ex != null)
							logger.error("Something went wrong while producing tweets - " + ex.getMessage());
					}
				});
			}
		}
		
		producer.flush();
		producer.close();

	}

	public Client run(BlockingQueue<String> msgQueue) {
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		List<String> terms = Lists.newArrayList("kafka");
		hosebirdEndpoint.trackTerms(terms);
		
		String apiKey = "key";
		String secretKey = "secret";
		String accessTokenKey = "access-token";
		String accessTokenSecret = "access-token secret";

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(apiKey, secretKey, accessTokenKey, accessTokenSecret);

		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();

		// Attempts to establish a connection.
		return hosebirdClient;
	}

}
