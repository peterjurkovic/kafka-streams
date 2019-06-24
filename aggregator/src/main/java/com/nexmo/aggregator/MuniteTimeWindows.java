package com.nexmo.aggregator;

import java.util.Map;

import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

public class MuniteTimeWindows extends Windows<TimeWindow> {

	@Override
	public Map<Long, TimeWindow> windowsFor(long timestamp) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long size() {
		// TODO Auto-generated method stub
		return 0;
	}

}
