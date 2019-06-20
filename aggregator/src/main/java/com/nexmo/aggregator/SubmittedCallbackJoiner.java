package com.nexmo.aggregator;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;

import com.codahale.metrics.Meter;

public class SubmittedCallbackJoiner {
	
	static {
		ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
	    root.setLevel(ch.qos.logback.classic.Level.INFO);
	}

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "agg");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		final StreamsBuilder builder = new StreamsBuilder();
		CallbackDeser.SerDe callbackDeser = new CallbackDeser.SerDe();
		MessagesDeser.SerDe messagesDeser = new MessagesDeser.SerDe();
		Serde<SubmittedMessage> submittedMessageSerde = new SubmittedMessageDeser().instance();
		
		KStream<String, Callback> submittedCallbacks = builder.stream("callbacks", Consumed.with(Serdes.String(), callbackDeser));
		KStream<String, Message> messages = builder.stream("messages", Consumed.with(Serdes.String(), messagesDeser));
			
		ValueJoiner<Callback, Message, SubmittedMessage> joiner = (c, m) -> new SubmittedMessage(c,m);
		
		final Meter meter = Metrics.submittedMeter();
		
		Joined<String, Callback, Message> joined = Joined.with(Serdes.String(), callbackDeser, messagesDeser);
		
		submittedCallbacks.filter((k,v)-> v.type.equals("submitted"))
				 .join(messages, joiner,  JoinWindows.of(1000*60*60*5), joined)
				 .map(new KeyValueMapper<String, SubmittedMessage, KeyValue<String, SubmittedMessage>>() {
					 
					 @Override
					public KeyValue<String, SubmittedMessage> apply(String key, SubmittedMessage value) {
						// key will be external id;
						meter.mark(); 
						return new KeyValue<String, SubmittedMessage>(value.getSubmitted().externalId, value);
					}
				})
				.to("submitted", Produced.with(Serdes.String(), submittedMessageSerde));
		
		
		final Topology topology = builder.build();


		org.apache.kafka.streams.TopologyDescription description = topology.describe();
		
		System.out.println(description);
		
		final KafkaStreams streams = new KafkaStreams(topology, props);
		final CountDownLatch latch = new CountDownLatch(1);

		// attach shutdown handler to catch control-c
		Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
			@Override
			public void run() {
				streams.close();
				latch.countDown();
			}
		});

		try {
			streams.start();
			latch.await();
		} catch (Throwable e) {
			System.exit(1);
		}
		System.exit(0);
	}
}

