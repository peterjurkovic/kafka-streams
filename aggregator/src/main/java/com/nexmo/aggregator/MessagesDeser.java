package com.nexmo.aggregator;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class MessagesDeser extends PojoDeser<Message> {

	public MessagesDeser() {
		super(Message.class);
	}

	public static class SerDe implements Serde<Message> {

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