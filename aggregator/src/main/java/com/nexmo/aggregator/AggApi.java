package com.nexmo.aggregator;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.nexmo.aggregator.AggLookup.Type;

import reactor.core.publisher.Mono;

@RestController
public class AggApi {

	@Autowired AggLookup lookupService;
	
	@GetMapping("/last-mo-check")
	public Mono<String> lastMo(@RequestParam String from, @RequestParam String to){
		return lookupService.check(from, to, Type.LAST_MO);	
	}
	
	@GetMapping("/volume-check")
	public Mono<String> volumeCheck(@RequestParam String from, @RequestParam String to){
		return lookupService.volumeCheck(from, to);	
	}
	
	@GetMapping("/print-latest-mo")
	public Mono<List<String>> lastMo(){
		return Mono.just(lookupService.printKeyValue(Type.LAST_MO));
	}	
	
	@GetMapping("/print-volume")
	public Mono<List<String>> volume(){
		return Mono.just(lookupService.printWindowed(Type.VOLUME));
	}	
	
}
