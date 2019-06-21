package com.nexmo.aggregator;

import static com.nexmo.aggregator.AggregatorApplication.MO_STORE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@Slf4j
@Component
public class AggLookup {


	private final InteractiveQueryService queryService;
	private final HostInfo thisHost;
	private final WebClient client = WebClient.create();
	
	public AggLookup(InteractiveQueryService iqs) {
		queryService = iqs;
		thisHost = iqs.getCurrentHostInfo();
	}
	
	
	public Mono<String> lookup(String from, String to){
		String key = from + "#" + to;
		
		HostInfo hostInfo = queryService.getHostInfo(MO_STORE, key, new StringSerializer());
		
		if (hostInfo.equals(thisHost)) {
			log.info("HIT! for key {}", key);
			Object found = queryService.getQueryableStore(MO_STORE, QueryableStoreTypes.keyValueStore());
			return Mono.just(String.valueOf(found != null));
			
		}
		
		String url = "http://" + hostInfo.host() + ":" + hostInfo.port() + "/check?to=" + to + "&from=" + from;
		log.info("Seinding remote request {}", url);
		return client.get()
					 .uri(url)
					 .retrieve()
					 .bodyToMono(String.class);
	}
	
	public List<String> localState(){
		ReadOnlyKeyValueStore<Object, ?> store = queryService.getQueryableStore(MO_STORE, QueryableStoreTypes.keyValueStore());
		
		if ( store == null) 
			return Collections.emptyList();
		
		KeyValueIterator<Object, ?> it = store.all();
				
		ArrayList<String> list = new ArrayList<>();
		while (it.hasNext()) {
			KeyValue<Object, ?> kv = it.next();
			list.add("key=" + kv.key +" - " + kv.value);
		}
		return list;
	}
}
