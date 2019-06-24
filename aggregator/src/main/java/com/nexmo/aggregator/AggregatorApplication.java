package com.nexmo.aggregator;

import static com.nexmo.aggregator.AggregatorApplication.LATEST_MO_STORE;
import static com.nexmo.aggregator.AggregatorApplication.WINDOW_SIZE_MS;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

import com.codahale.metrics.Meter;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.repository.annotation.Field;
import com.couchbase.client.java.repository.annotation.Id;
import com.nexmo.aggregator.domain.ByCountryAggregates;
import com.nexmo.aggregator.domain.Callback;
import com.nexmo.aggregator.domain.Mo;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@EnableBinding(Bindings.class)
@SpringBootApplication
public class AggregatorApplication {

	public static final String LATEST_MO_STORE = "LatestMo";
	public static final String MT_BY_COUNTRY_STORE = "CountByCountry";
	public static final int WINDOW_SIZE_MS = 60_000 * 60 * 5;
	
	public static void main(String[] args) {
		SpringApplication.run(AggregatorApplication.class, args);
	}
}

@Slf4j
@Component
class LatestMoAggregator {
	
	@SendTo(Bindings.MO_LATEST_OUT)
	@StreamListener(Bindings.MO)
	public KStream<?, ?> aggregateLastMo(KStream<String, Mo> messages) {
	
		 return  messages.selectKey((key,message) -> message.from + "#" + message.to)
						 .map((k,v) -> new KeyValue<>(k, v.receivedAt.toString()))
						 .groupByKey()
						 .reduce((oldTimestamp, newTimestamp) -> newTimestamp, Materialized.as(LATEST_MO_STORE))
						 .toStream();
	}
}

@Slf4j
@Component
class DeliveredMtByCountry {
	
	@SendTo(Bindings.MT_COUNTRY_COUNT)
	@StreamListener(Bindings.CALLBACKS)
	public KStream<?, ?> deliverdCount(KStream<String, Callback> callbacks) {
	Meter meter = Metrics.agg();
	return callbacks.filter((k,callback) -> callback.type.equals("delivered"))
		    			   .map((k,v) -> new KeyValue<>(v.from +"#"+v.to, v.messageid))
					 	   .groupByKey(Serialized.with(Serdes.String(), Serdes.String()))
					 	   // .windowedBy(TimeWindows.of(WINDOW_SIZE_MS).advanceBy(WINDOW_SIZE_MS).until(WINDOW_SIZE_MS*2))
					 	   .windowedBy(new DailyTimeWindows(ZoneId.of("UTC"), 0, Duration.ZERO))
					 	   .count(Materialized.<String, Long, WindowStore<Bytes,byte[]>>as(AggregatorApplication.MT_BY_COUNTRY_STORE).withCachingEnabled())
					 	   
					 	   .toStream()
					 	   .peek((k,v) -> meter.mark())
					 	   .map(new KeyValueMapper<Windowed<String>, Long, KeyValue<String,ByCountryAggregates>>() { 
					 			@Override
					 			public KeyValue<String, ByCountryAggregates> apply(Windowed<String> window, Long value) {
					 				ByCountryAggregates agg = new ByCountryAggregates();
					 				String[] key = window.key().split("#");
					 				if (key.length != 2) throw new IllegalStateException("Unexpected key");
					 				agg.setCount(value);
					 				agg.setStart(window.window().start());
					 				agg.setEnd(window.window().end());
					 				agg.setFrom(key[0]);
					 				agg.setTo(key[1]);
					 				return new KeyValue<String, ByCountryAggregates>(window.key(), agg);
					 			}
							});		 		 
		
	}
}

@Slf4j
@ConditionalOnBean(Bucket.class)
@Component
class LatestMoCouchbase {
	
	@Autowired Bucket bucket;
	
	@StreamListener(Bindings.MO_COUCHBASE)
	public void couchbaseMoInserter(KStream<String, Mo> messages) {
		Meter meter = Metrics.couchbase();
		 messages.foreach((k,v) -> {
			 int expiry = (int) v.receivedAt.plusSeconds(60).toEpochMilli() / 1000;
			 JsonObject json = JsonObject.empty().put("receivedAt", v.receivedAt.toEpochMilli());
			 bucket.upsert(JsonDocument.create(v.from + "#" + v.to, expiry, json));
			 meter.mark();
		 });
		
	}
}

interface Bindings{
	

	String CALLBACKS = "callbacks";
	
	String MO = "mo";
	
	String MO_COUCHBASE = "mo-cb";
	
	String MO_LATEST_OUT = "molatest";
	
	String MT_COUNTRY_COUNT = "mt-country";
	
	@Input(MO)
	KStream<String, Mo> mo();
	
//	@Input(MO_COUCHBASE)
//	KStream<String, Mo> moCb();
	
	@Input(CALLBACKS)
	KStream<String, Callback> callbacks();
	
	@Output(MO_LATEST_OUT)
	KStream<?, ?> latestMo();
	
	@Output(MT_COUNTRY_COUNT)
	KStream<?, ?> mtCount();
	
}


@Data
class FromTo{
	@Id String id;
	@Field Instant receivedAt;
}
