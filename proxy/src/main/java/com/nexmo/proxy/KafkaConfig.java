package com.nexmo.proxy;

import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {
	
	@Value("${kafka.host:localhost}")
	String host;
	
	@Value("${kafka.port:9092}")
	String port;
	
	
	@Bean
	public KafkaStreams kafkaStreams() {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "agg2");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		Topology topology = new Topology();
		KafkaStreams streams = new KafkaStreams(topology, props);
		streams.start();
		return streams;
	}

}
