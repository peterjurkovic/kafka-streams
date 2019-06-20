package com.nexmo.chatapp;

import java.util.UUID;
import java.util.function.Supplier;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.thedeanda.lorem.LoremIpsum;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
public class ChatappApplication {

	public static void main(String[] args) {
		SpringApplication.run(ChatappApplication.class, args);
	}

	@Data
	static class Message{
		
		String id = UUID.randomUUID().toString();
		String from;
		String to;
		String price = "1";
		String cost = "1";
		String country;
		
		Message(String from, String to, String country){
			this.from = from;
			this.to = to;
			this.country = country;
		}
	}
	
	@Slf4j
	@Component
	static class MessageProducer {
		
		final KafkaTemplate<String, Message> template;
		final LoremIpsum gen = LoremIpsum.getInstance();

		public MessageProducer(KafkaTemplate<String, Message> template) {
			super();
			this.template = template;
		}
		
			@Async
			public void produce(int count, String from, String to, String country) {
				long start = System.currentTimeMillis();
				for(int i = 1; i <= count; i++) {
					
					if (i % 10000 == 0)
						log.info("Producing {} of {}", i, count );
					
					Message message = new Message(getOrDefaul(from, gen::getPhone),
												  getOrDefaul(to, gen::getPhone),
							                      getOrDefaul(country, gen::getCountry));
					
					template.send("messages", message.getId(), message);
				}
				
				long end = System.currentTimeMillis();
				
				log.info("Producing of {} finished in {} ", count, (end -start));
			}
			
			static String getOrDefaul(String val, Supplier<String> f) {
				return val == null ? f.get() : val;
			}
			
		}
	
		@Slf4j
		@RestController
		static class Api{
			
			MessageProducer producer;
			
			
			public Api(MessageProducer producer) {
				this.producer = producer;
			}

			@ResponseStatus(HttpStatus.ACCEPTED)
			@GetMapping("/produce/{count}")
			void produce(@PathVariable int count,
						 @RequestParam(required = false) String to,
						 @RequestParam(required = false) String from, 
						 @RequestParam(required = false) String country) {
				
				log.info("Producing Count {} to {} from {} {} country", count, to, from, country);
				
				producer.produce(count, from, to, country);
			}
		}
	
}
	
	
	

