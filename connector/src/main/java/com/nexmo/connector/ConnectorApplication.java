package com.nexmo.connector;

import java.time.Instant;
import java.util.Arrays;
import java.util.UUID;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@EnableBinding(Bindings.class)
@SpringBootApplication
public class ConnectorApplication {

	public static void main(String[] args) {
		SpringApplication.run(ConnectorApplication.class, args);
	}

}

@Slf4j
@Component
class MessageListener {
	
	@SendTo(Bindings.CALLBACKS)
	@StreamListener(Bindings.MESSAGES)
	public KStream<String, Callback> process(KStream<String, Message> messages) {
		
		return messages.flatMap(new KeyValueMapper<String, Message, Iterable<KeyValue<String,Callback>>>() {
			@Override
			public Iterable<KeyValue<String, Callback>> apply(String key, Message m) {
				String eid = "e-" +UUID.randomUUID().toString();
				
				Callback submitted = new Callback(m.getId(), eid, "submitted", Instant.now());
				KeyValue<String, Callback> submittedKeyValue = new KeyValue<>(m.getId(), submitted);
				
				Callback deliverd = new Callback(null, eid, "delivered", Instant.now().plusMillis(100));
				KeyValue<String, Callback> deliverdKeyValue = new KeyValue<>(eid, deliverd);
				
				return Arrays.asList(submittedKeyValue, deliverdKeyValue);
			}
		});
		
	}
}

@Data
@AllArgsConstructor
class Callback{
	
	String messageid, externalId, type;
	Instant receivedAt;
}

@Data
class Message{
	
	String id, from, to, price, cost, country;
	
}
	
interface Bindings{
	
	String MESSAGES = "messages";

	String CALLBACKS = "callbacks";
	
	@Input(MESSAGES)
	KStream<String, String> messages();
	
	@Output(CALLBACKS)
	KStream<String, Callback> callbacks();
	
}
