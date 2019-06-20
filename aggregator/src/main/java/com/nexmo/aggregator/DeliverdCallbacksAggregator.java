package com.nexmo.aggregator;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.Windowed;

import com.codahale.metrics.Meter;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class DeliverdCallbacksAggregator {

	static {
		ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
	    root.setLevel(ch.qos.logback.classic.Level.INFO);
	}

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "agg2");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		final StreamsBuilder builder = new StreamsBuilder();
		CallbackDeser.SerDe callbackDeser = new CallbackDeser.SerDe();
		Serde<SubmittedMessage> submittedMessageSerde = new SubmittedMessageDeser().instance();
		
		KStream<String, Callback> callbacks = builder.stream("callbacks", Consumed.with(Serdes.String(), callbackDeser));
		KStream<String, SubmittedMessage> submitted = builder.stream("submitted", Consumed.with(Serdes.String(), submittedMessageSerde));
		
	
		ValueJoiner<Callback, SubmittedMessage, SubmittedMessage> deliveredJoiner = (c, m) -> m;
		
		Joined<String, Callback, SubmittedMessage > deliveredJoined = Joined.with(Serdes.String(), callbackDeser, submittedMessageSerde);
		
		final Meter delivered = Metrics.deliveredMetter();
	callbacks
		 .filter((k,v) -> v.type.equals("delivered"))
		 .join(submitted, deliveredJoiner,  JoinWindows.of(1000*60*60*5), deliveredJoined)
		 .selectKey((k,v) -> v.getMessage().from +"#" + v.getMessage().country )
		 .groupByKey(Serialized.with(Serdes.String(), submittedMessageSerde))
		 .windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(1)))
		 .count()
		 .toStream()
		 .map(new KeyValueMapper<Windowed<String>, Long, KeyValue<String, ByCountryAndSender>>() {
             
			 @Override	
             public KeyValue<String, ByCountryAndSender> apply(Windowed<String> window, Long value) {
            	 String[] k = window.key().split("#");
            	 
            	 ByCountryAndSender agg = new ByCountryAndSender();
            	 agg.setCount(value);
				 agg.setCountry(k[1]);
				 agg.setFrom(k[0]);
				 agg.setStart(window.window().start());
				 agg.setEnd(window.window().end());
				 
				 delivered.mark();
                 return new KeyValue<>(window.key(), agg);
             }
         }).to("agg-recipient-country", Produced.with(Serdes.String(), new ByCountryAndSenderDeser().instance()));

		
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

@Data
@AllArgsConstructor
@NoArgsConstructor
class ByCountryAndSender{
	long start;
	long end;
	String country;
	String from;
	Long count;
}

class ByCountryAndSenderDeser extends PojoDeser<ByCountryAndSender> {

	public ByCountryAndSenderDeser() {
		super(ByCountryAndSender.class);
	}

}