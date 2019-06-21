package com.nexmo.aggregator;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

public class Metrics {


	
	static final MetricRegistry metrics = new MetricRegistry();
	static final ConsoleReporter reporter;
	
	static {
		reporter = ConsoleReporter.forRegistry(metrics)
			       .convertRatesTo(TimeUnit.SECONDS)
			       .convertDurationsTo(TimeUnit.MILLISECONDS)
			       .build();
		
		reporter.start(5, TimeUnit.SECONDS);
	}
	
	
	public static Meter submittedMeter() {
		return metrics.meter("submitted-join");

	}
	
	public static Meter deliveredMetter() {
		return metrics.meter("delivered-join");

	}
	
	public static Meter couchbase() {
		return metrics.meter("couchbase-lastMo");

	}
}
