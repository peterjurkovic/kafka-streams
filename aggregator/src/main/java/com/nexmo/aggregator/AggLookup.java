package com.nexmo.aggregator;

import static com.nexmo.aggregator.AggregatorApplication.LATEST_MO_STORE;
import static com.nexmo.aggregator.AggregatorApplication.MT_BY_COUNTRY_STORE;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.CleanupConfig;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@Slf4j
@Component
public class AggLookup {
	
	enum Type{
		LAST_MO("last-mo-check", LATEST_MO_STORE),
		VOLUME("volume-check", MT_BY_COUNTRY_STORE);
		
		String path, store;
		
		private Type(String action, String store) {
			this.path = action;
			this.store = store;
		}
	}

	private final InteractiveQueryService queryService;
	private final HostInfo thisHost;
	private final WebClient client = WebClient.create();
	private final ZoneId UTC = ZoneId.of("UTC");
	
	public AggLookup(InteractiveQueryService iqs) {
		queryService = iqs;
		thisHost = iqs.getCurrentHostInfo();
	}
		
	public Mono<String> check(String from, String to, Type action){
		String key = from + "#" + to;
		
		HostInfo hostInfo = queryService.getHostInfo(action.store, key, new StringSerializer());
		
		if (hostInfo.equals(thisHost)) {
			log.info("HIT! for key {}", key);
			Object found = queryService.getQueryableStore(action.store, QueryableStoreTypes.keyValueStore());;
			return Mono.just(String.valueOf(found != null));			
		}
		
		String url = "http://" + hostInfo.host() + ":" + hostInfo.port() + "/"+action.path+"?to=" + to + "&from=" + from;
		log.info("Seinding remote request {}", url);
		return client.get()
					 .uri(url)
					 .retrieve()
					 .bodyToMono(String.class);
	}
	
	public Mono<String> volumeCheck(String from, String to) {
		String key = from + "#" + to;
		
		HostInfo hostInfo = queryService.getHostInfo(Type.VOLUME.store, key, new StringSerializer());
		
		if (hostInfo.equals(thisHost)) {
			log.info("HIT! for key {}", key); 
			ReadOnlyWindowStore<Object, ?> store = queryService.getQueryableStore(Type.VOLUME.store, QueryableStoreTypes.windowStore());
			long start = LocalDate.now().atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
			Object result = store.fetch(key, start);
			return Mono.just(String.valueOf(result));			
		}
		
		String url = "http://" + hostInfo.host() + ":" + hostInfo.port() + "/"+Type.VOLUME.path+"?to=" + to + "&from=" + from;
		log.info("Sending remote request {}", url);
		return client.get()
					 .uri(url)
					 .retrieve()
					 .bodyToMono(String.class);
		
	}
	
	public List<String> printKeyValue(Type type){
		log.info("Printing printKeyValue {}", type.store);
		ReadOnlyKeyValueStore<Object, ?> store = queryService.getQueryableStore(type.store, QueryableStoreTypes.keyValueStore());
		
		if ( store == null) {
			log.info("printKeyValue {} not found", type.store);
			return Collections.emptyList();
		}
		
		KeyValueIterator<Object, ?> it = store.all();
				
		ArrayList<String> list = new ArrayList<>();
		while (it.hasNext()) {
			KeyValue<Object, ?> kv = it.next();
			list.add("key=" + kv.key +" - " + kv.value);
		}
		return list;
	}
	
	
	public List<String> printWindowed(Type type){
		log.info("Printing printWindowed {}", type.store);
			
		ReadOnlyWindowStore<Object, ?> store = queryService.getQueryableStore(type.store, QueryableStoreTypes.windowStore());
		
		if ( store == null) {
			log.info("printWindowed {} not found", type.store);
			return Collections.emptyList();
		}
		
		KeyValueIterator<Windowed<Object>, ?> it = store.fetchAll(Instant.MIN.getEpochSecond(), Instant.MAX.getEpochSecond());
		ArrayList<String> list = new ArrayList<>();
		while (it.hasNext()) {
			KeyValue<Windowed<Object>, ?> kv = it.next();
			list.add("key=" + kv.key.key() +" - " + kv.value + 
					" window [" + asString(kv.key.window().start()) + " - "  +asString(kv.key.window().end())+"]" + 
					" count = " + kv.value);
		}
		
		return list;
	}
	
	private static String asString(long timestamp) {
		return Instant.ofEpochMilli(timestamp).toString();
	}
}


