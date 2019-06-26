package com.nexmo.couchbase;

import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.repository.CrudRepository;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.couchbase.client.java.repository.annotation.Field;
import com.couchbase.client.java.repository.annotation.Id;
import com.thedeanda.lorem.LoremIpsum;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@EnableAsync
@EnableCouchbaseRepositories
@SpringBootApplication
public class CouchbaseApplication {

	public static void main(String[] args) {
		SpringApplication.run(CouchbaseApplication.class, args);
	}
}

@Slf4j
@Component
class MessagePopulator {
	
	@Autowired
	MessageRepository repository;
	
	final LoremIpsum gen = LoremIpsum.getInstance();
	
	@Async
	public void populate(int count) {
		for(int i = 0; i < count; i++) {
			Message m = new Message(UUID.randomUUID().toString(),
									gen.getPhone(),
									gen.getPhone(),
									"1",
									"1",
									gen.getCountry());
			
			repository.save(m);
			
			if (i % 10000 == 1)
				log.info("Populated {} of {}", i, count);
		}
		
		log.info("DONE Populated {} ",count);
	}
}

@RestController
class Api {
	
	@Autowired MessagePopulator populator;
	
	@ResponseStatus(value = HttpStatus.ACCEPTED)
	@PostMapping("/populate/{count}")
	void populate(@PathVariable int count) {
		populator.populate(count);
	}
}

@Repository	
interface MessageRepository extends CrudRepository<Message, String> {}
	
@Data
@Document
@AllArgsConstructor
@NoArgsConstructor
class Message{
	@Id String id;
	@Field  String from, to, price, cost, country;
}
