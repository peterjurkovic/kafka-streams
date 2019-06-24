package com.nexmo.client;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class PojoDeser<T> implements Deserializer<T>, Serializer<T> {

	static final ObjectMapper mapper = new ObjectMapper()
			.registerModule(new JavaTimeModule())
			.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);

	private final Class<T> clazz;

	public PojoDeser(Class<T> clazz) {
		super();
		this.clazz = clazz;
	}

	@Override
	public byte[] serialize(String topic, T data) {
		try {
			return mapper.writeValueAsBytes(data);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
	}

	@Override
	public T deserialize(String topic, byte[] data) {
		try {
			return mapper.readValue(data, clazz);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() {
	}
	
	public SerDe instance(){
		return new SerDe();
	}
	
	public class SerDe implements Serde<T> {

		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {

		}

		@Override
		public void close() {
			PojoDeser.this.close();
		}

		@Override
		public Serializer<T> serializer() {
			return PojoDeser.this;
		}

		@Override
		public Deserializer<T> deserializer() {
			return PojoDeser.this;
		}
	}
}
