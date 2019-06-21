package com.nexmo.aggregator.domain;

import java.time.Instant;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Mo {
	public String id, from, to, country;
	public Instant receivedAt;
}