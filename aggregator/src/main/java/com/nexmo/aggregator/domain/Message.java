package com.nexmo.aggregator.domain;

import lombok.Data;

@Data
public class Message{
	public String id, from, to, price, cost, country;	
}