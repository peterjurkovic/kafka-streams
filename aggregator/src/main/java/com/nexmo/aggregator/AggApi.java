package com.nexmo.aggregator;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import reactor.core.publisher.Mono;

@RestController
public class AggApi {

	@Autowired AggLookup lookupService;
	
	@GetMapping("/check")
	public Mono<String> check(@RequestParam String from, @RequestParam String to){
		return lookupService.lookup(from, to);	
	}
	
	@GetMapping("/print")
	public Mono<List<String>> print(){
		return Mono.just(lookupService.localState());
	}
}
