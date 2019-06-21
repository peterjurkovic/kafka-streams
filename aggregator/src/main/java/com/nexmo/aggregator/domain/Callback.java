package com.nexmo.aggregator.domain;

import java.time.Instant;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Callback{
	public String messageid, externalId, type;
	public Instant receivedAt;
}