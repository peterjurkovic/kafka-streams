package com.nexmo.connector;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MessagesDeser implements Deserializer<Message>, Serializer<Message>{
	static final ObjectMapper mapper = new ObjectMapper();
	
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {}

	@Override
	public Message deserialize(String topic, byte[] data) {
		try {
			return mapper.readValue(data, Message.class);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() {}

	@Override
	public byte[] serialize(String topic, Message data) {
		try {
			return mapper.writeValueAsBytes(data);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static class SerDe implements Serde<Message>{

		private final MessagesDeser serdes = new MessagesDeser();
		
		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {
			
		}

		@Override
		public void close() {
			serdes.close();
		}

		@Override
		public Serializer<Message> serializer() {
			return serdes;
		}

		@Override
		public Deserializer<Message> deserializer() {
			return serdes;
		}
		
	}
	
}