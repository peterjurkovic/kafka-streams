package com.nexmo.aggregator;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

public class DailyTimeWindows extends Windows<TimeWindow>{

    private final ZoneId zoneId;
    private final long grace;
    private final int startHour;

    public DailyTimeWindows(final ZoneId zoneId, final int startHour, final Duration grace) {
        this.zoneId = zoneId;
        this.grace = grace.toMillis();
        this.startHour = startHour;
    }

    @Override
    public Map<Long, TimeWindow> windowsFor(final long timestamp) {
        final Instant instant = Instant.ofEpochMilli(timestamp);

        final ZonedDateTime zonedDateTime = instant.atZone(zoneId);
        final ZonedDateTime startTime = zonedDateTime.getHour() >= startHour ? zonedDateTime.truncatedTo(ChronoUnit.DAYS).withHour(startHour) : zonedDateTime.truncatedTo(ChronoUnit.DAYS).minusDays(1).withHour(startHour);
        final ZonedDateTime endTime = startTime.plusDays(1);

        final Map<Long, TimeWindow> windows = new LinkedHashMap<>();
        windows.put(toEpochMilli(startTime), new TimeWindow(toEpochMilli(startTime), toEpochMilli(endTime)));
        return windows;
    }

    @Override
    public long maintainMs() {
        //By default the Window class have a maintainMs = 1 day
        //So we need to make sure retention is at least than size + grace period
        //Otherwise sanity check made by TimeWindowedKStreamImpl.materialize() method will throw an Exception

        //NOTE: that this could be done either in the window it self or by configuring retention on the Materialize by calling Materialize.withRetention(XXX)
        return size();
    }

    @Override
    public long size() {
        return Duration.ofDays(1).toMillis();
    }

    private long toEpochMilli(final ZonedDateTime zonedDateTime) {
        return zonedDateTime.toInstant().toEpochMilli();
}

	@Override
	public long gracePeriodMs() {
		return 100;
	}
}
