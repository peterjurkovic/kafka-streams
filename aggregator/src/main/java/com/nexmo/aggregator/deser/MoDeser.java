package com.nexmo.aggregator.deser;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.nexmo.aggregator.domain.Mo;

public class MoDeser extends PojoDeser<Mo>{

	public MoDeser() {
		super(Mo.class);
		// TODO Auto-generated constructor stub
	}

	public static class SerDe implements Serde<Mo> {

		private final MoDeser serdes = new MoDeser();

		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {

		}

		@Override
		public void close() {
			serdes.close();
		}

		@Override
		public Serializer<Mo> serializer() {
			return serdes;
		}

		@Override
		public Deserializer<Mo> deserializer() {
			return serdes;
		}

	}
}
