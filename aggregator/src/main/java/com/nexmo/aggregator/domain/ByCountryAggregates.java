package com.nexmo.aggregator.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ByCountryAggregates {
	long start;
	long end;
	String from;
	String to;
	Long count;
}
