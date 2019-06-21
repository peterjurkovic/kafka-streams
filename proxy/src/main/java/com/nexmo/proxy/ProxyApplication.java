package com.nexmo.proxy;

import java.util.Collection;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@SpringBootApplication
public class ProxyApplication {

	public static void main(String[] args) {
		SpringApplication.run(ProxyApplication.class, args);
	}

	
}

@Slf4j
@Component
class Proxy{
	
	private final KafkaStreams streams;
	
	WebClient client = WebClient.create();
	
	public Proxy(KafkaStreams streams) {
		this.streams = streams;		
	}
	
	public Mono<String> find(String from, String to){

		Collection<StreamsMetadata> meta = streams.allMetadataForStore("molatest");
		StreamsMetadata metadata = streams.metadataForKey("molatest", "from" + "#" + "to", Serdes.String().serializer());
		
		String url = "http://"+metadata.host() + ":" + metadata.port() + "/check?from="+from+"&to="+to;
		log.info("Quering {}", url);
		return client.get()
					  .uri(url)
					  .accept(MediaType.APPLICATION_JSON)
					  .retrieve()
					  .bodyToMono(String.class);
		
	}
	
}

@RestController
class Api{
	
	@Autowired Proxy proxy;
	
	@GetMapping("/check")
	public Mono<String> check(@RequestParam String from, @RequestParam String to) {
		return proxy.find(from, to);
	}
}

