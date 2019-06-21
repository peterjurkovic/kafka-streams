package com.nexmo.aggregator.deser;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.nexmo.aggregator.domain.Callback;

public class CallbackDeser extends PojoDeser<Callback> {

	public CallbackDeser() {
		super(Callback.class);
	}

	public static class SerDe implements Serde<Callback> {

		private final CallbackDeser serdes = new CallbackDeser();

		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {

		}

		@Override
		public void close() {
			serdes.close();
		}

		@Override
		public Serializer<Callback> serializer() {
			return serdes;
		}

		@Override
		public Deserializer<Callback> deserializer() {
			return serdes;
		}

	}
}
