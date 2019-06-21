package com.nexmo.aggregator;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.couchbase.client.java.repository.annotation.Field;
import com.couchbase.client.java.repository.annotation.Id;
import com.nexmo.aggregator.domain.Callback;
import com.nexmo.aggregator.domain.Mo;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@EnableBinding(Bindings.class)
@SpringBootApplication
public class AggregatorApplication {

	static final String MO_STORE = "latest-mo";
	
	public static void main(String[] args) {
		SpringApplication.run(AggregatorApplication.class, args);
	}

}


@Slf4j
@Component
class LatestMoAggregator {
	
	@SendTo(Bindings.MO_STORE)
	@StreamListener(Bindings.MO)
	public KStream<?, ?> aggregateLastMo(KStream<String, Mo> messages) {
	
		 return  messages.selectKey((key,message) -> message.from + "#" + message.to)
						 .map((k,v) -> new KeyValue<>(k, v.receivedAt.toString()))
						 .groupByKey()
						 .reduce((oldTimestamp, newTimestamp) -> newTimestamp,
							        Materialized.as(AggregatorApplication.MO_STORE))
						 .toStream();
		
	}
}

//@Slf4j
//@ConditionalOnBean(Bucket.class)
//@Component
//class CouchbaseMo {
//	
//	@Autowired Bucket bucket;
//	
//	@StreamListener(Bindings.MO_CB)
//	public void couchbaseMoInserter(KStream<String, Mo> messages) {
//		Meter meter = Metrics.couchbase();
//		 messages.foreach((k,v) -> {
//			 
//			 int expiry = (int) v.receivedAt.plusSeconds(60).toEpochMilli() / 1000;
//			 JsonObject json = JsonObject.empty().put("receivedAt", v.receivedAt.toEpochMilli());
//
//			 bucket.upsert(JsonDocument.create(v.from + "#" + v.to, expiry, json));
//			 meter.mark();
//		 });
//		
//	}
//}

interface Bindings{
	

	String CALLBACKS = "callbacks";
	
	String MO = "mo";
	
	String MO_CB = "mo-cb";
	
	String MO_STORE = "molatest";
	
	@Input(MO)
	KStream<String, Mo> mo();
	
	@Input(MO_CB)
	KStream<String, Mo> moCb();
	
	@Input(CALLBACKS)
	KStream<String, Callback> callbacks();
	
	@Output(MO_STORE)
	KStream<?, ?> aggMo();
	
}

//@Repository
//interface FromToRepository extends CrudRepository<FromTo, String> {}

@Data
class FromTo{
	@Id String id;
	@Field Instant receivedAt;
}
