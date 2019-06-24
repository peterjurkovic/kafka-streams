package com.nexmo.client;

import java.time.Instant;

import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import com.codahale.metrics.Timer;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@EnableBinding(Bindings.class)
@SpringBootApplication
public class ClientApplication {

	public static void main(String[] args) {
		SpringApplication.run(ClientApplication.class, args);
	}

}

@Slf4j
@Component
class CallbackProcessor {

	private final WebClient client = WebClient.create();
	
	@Value("${aggregator.url}")
	public String baseUrl;
	
	@StreamListener(Bindings.CALLBACKS)
	public void process(KStream<String, Callback> callbacks) {
		Timer timer = Metrics.requestTimer();
		callbacks.foreach((k, cb) -> {
			Timer.Context ctx = timer.time();
			String url = baseUrl +"/volume-check?from=" + cb.from + "&to=" + cb.to;
			
			client.get()
				  .uri(url)
				  .retrieve()
				  .bodyToMono(String.class)
				  .doOnError( e -> { log.error(e.getMessage());ctx.stop();})
				  .doOnSuccess(s -> {log.info(s); ctx.stop();})
				  .subscribe();
				  
		});
	}
}

@Data
@NoArgsConstructor
class Callback {
	public String messageid, externalId, type, from, to;
	public Instant receivedAt;
}

interface Bindings {
	String CALLBACKS = "callbacks";

	@Input(CALLBACKS)
	KStream<?, ?> input();
}
