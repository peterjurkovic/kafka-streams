package com.nexmo.client;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

public class Metrics {


	
	static final MetricRegistry metrics = new MetricRegistry();
	static final ConsoleReporter reporter;
	
	static {
		reporter = ConsoleReporter.forRegistry(metrics)
			       .convertRatesTo(TimeUnit.SECONDS)
			       .convertDurationsTo(TimeUnit.MILLISECONDS)
			       .build();
		
		reporter.start(10, TimeUnit.SECONDS);
	}
	
	
	public static Meter requestMeter() {
		return metrics.meter("requests");
	}
	
	public static Timer requestTimer() {
		return metrics.timer("requests-timer");
	}
	
}
