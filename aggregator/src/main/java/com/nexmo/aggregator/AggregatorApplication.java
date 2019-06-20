package com.nexmo.aggregator;

import java.time.Instant;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@EnableBinding(Bindings.class)
@SpringBootApplication
public class AggregatorApplication {

	public static void main(String[] args) {
		SpringApplication.run(AggregatorApplication.class, args);
	}

}


@Slf4j
@Component
class CallbckProcessor {
	
	@SendTo(Bindings.SUBMITTED_MESSAGE)
	@StreamListener
	public KStream<String, SubmittedMessage> joinSubmitted(
			@Input(Bindings.MESSAGES) KStream<String, Message> messages,
			@Input(Bindings.CALLBACKS) KStream<String, Callback> callbacks) {
		
			ValueJoiner<Callback, Message, SubmittedMessage> joiner = (c, m) -> new SubmittedMessage(c,m);
			return callbacks//.mapValues( c -> {log.info(c.toString());return c;}) 
							// .filter((k,v) -> v.type.equals("submitted"))
							// .through("correct")
							 
							.join(messages, joiner,  JoinWindows.of(1000*60*60*24)	);
	}
	

//	@StreamListener(Bindings.CALLBACKS)
//	public void cb( KStream<String, Callback> cb) {
//		cb.print(Printed.toSysOut());
//	}
}

@Data
class Callback{
	
	String messageid, externalId, type;
	Instant receivedAt;
	
}

@Data
class Message{
	
	String id, from, to, price, cost, country;
	
}

@Data
@NoArgsConstructor
@AllArgsConstructor
class SubmittedMessage{
	Callback submitted;
	Message message;
}


	
interface Bindings{
	
	String MESSAGES = "messages";

	String CALLBACKS = "callbacks";
	
	String SUBMITTED_MESSAGE = "submitted";
	
	@Input(MESSAGES)
	KStream<String, Message> messages();
	
	@Input(CALLBACKS)
	KStream<String, Callback> callbacks();
	
	@Output(SUBMITTED_MESSAGE)
	KStream<String, SubmittedMessage> submitted();
	
	
}
