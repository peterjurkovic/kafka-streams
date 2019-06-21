package com.nexmo.connector;

import java.time.Instant;
import java.util.Arrays;
import java.util.UUID;
import java.util.function.Supplier;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.thedeanda.lorem.LoremIpsum;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@EnableAsync
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
				
				Callback submitted = new Callback(m.getId(), eid, "submitted", m.from, m.to, Instant.now());
				KeyValue<String, Callback> submittedKeyValue = new KeyValue<>(m.getId(), submitted);
				
				Callback deliverd = new Callback(null, eid, "delivered", m.from, m.to, Instant.now().plusMillis(100));
				KeyValue<String, Callback> deliverdKeyValue = new KeyValue<>(eid, deliverd);
				
				return Arrays.asList(submittedKeyValue, deliverdKeyValue);
			}
		});
		
	}
}


@Slf4j
@Component
class MessageProducer {
	
	final KafkaTemplate<String, Mo> template;
	final LoremIpsum gen = LoremIpsum.getInstance();

	public MessageProducer(KafkaTemplate<String, Mo> template) {
		super();
		this.template = template;
	}
	
		@Async
		public void produce(int count, String from, String to, String country) {
			long start = System.currentTimeMillis();
			for(int i = 1; i <= count; i++) {
				
				if (i % 10000 == 0)
					log.info("Producing {} of {}", i, count );
				
				Mo message = new Mo(UUID.randomUUID().toString(),
									getOrDefaul(from, gen::getPhone),
									getOrDefaul(to, gen::getPhone),
						            getOrDefaul(country, gen::getCountry),
						            Instant.now());
				
				template.send("messages-mo", message.getId(), message);
			}
			long end = System.currentTimeMillis();
			log.info("Producing of {} finished in {} ", count, (end-start));
		}
		
		static String getOrDefaul(String val, Supplier<String> f) {
			return val == null ? f.get() : val;
		}
		
	}

@Slf4j
@RestController
class Api{
	
	MessageProducer producer;
	
	
	public Api(MessageProducer producer) {
		this.producer = producer;
	}

	@ResponseStatus(HttpStatus.ACCEPTED)
	@GetMapping("/produce/{count}")
	void produce(@PathVariable int count,
			 	@RequestParam(required = false) String from,
				@RequestParam(required = false) String to,
				@RequestParam(required = false) String country) {
		
		log.info("Producing Count {} from {} to {} {} country", count, from, to, country);
		
		producer.produce(count, from, to, country);
	}
}

@Data
@AllArgsConstructor
class Callback{
	String messageid, externalId, type, from, to;
	Instant receivedAt;
}

@Data
class Message{
	String id, from, to, price, cost, country;
}

@Data
@AllArgsConstructor
class Mo {
	String id, from, to, country;
	Instant receivedAt;
}
	
interface Bindings{
	
	String MESSAGES = "messages";

	String CALLBACKS = "callbacks";
	
	@Input(MESSAGES)
	KStream<String, String> messages();
	
	@Output(CALLBACKS)
	KStream<String, Callback> callbacks();
	
}
