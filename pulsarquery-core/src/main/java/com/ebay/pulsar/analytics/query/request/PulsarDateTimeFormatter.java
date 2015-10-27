/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.query.request;

import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.ISODateTimeFormat;
import org.joda.time.format.ISOPeriodFormat;
import org.joda.time.format.PeriodFormatter;

import com.google.common.collect.Lists;

/**
 * 
 * @author mingmwang
 *
 */
public class PulsarDateTimeFormatter {

	private static DateTimeParser[] inputTimeFormats = {
			DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").getParser(),
			DateTimeFormat.forPattern("yyyy-MM-dd HH:mm").getParser(),
			DateTimeFormat.forPattern("yyyy-MM-dd").getParser() };

	public static DateTimeZone MST_TIMEZONE = DateTimeZone.forID("MST");
	
	public static DateTimeFormatter INPUTTIME_FORMATTER = new DateTimeFormatterBuilder()
			.append(null, inputTimeFormats).toFormatter().withZone(MST_TIMEZONE);
	
	public static DateTimeFormatter OUTPUTTIME_FORMATTER = DateTimeFormat
			.forPattern("yyyy-MM-dd HH:mm:ss").withZone(MST_TIMEZONE);
	
	public static DateTimeFormatter INPUT_DRUID_TIME_FORMATTER = DateTimeFormat
			.forPattern("yyyy-MM-dd'T'HH:mm:ss-07:00").withZone(MST_TIMEZONE);
	
	public static DateTimeFormatter OUTPUT_DRUID_TIME_FORMATTER = ISODateTimeFormat
			.dateTimeParser().withZone(MST_TIMEZONE);
	
	public static PeriodFormatter ISO_PERIOD_FORMATTER = ISOPeriodFormat.standard();
	
	
	public static List<String> buildStringIntervals(DateRange intervals) {
		List<String> strIntervals = Lists.newArrayList();
		DateTime start = intervals.getStart();
		DateTime end = intervals.getEnd();
		strIntervals.add(INPUT_DRUID_TIME_FORMATTER.print(start) + "/" + INPUT_DRUID_TIME_FORMATTER.print(end));
		return strIntervals;
	}
	
	public static DateRange parseIntevalsFromString(String strIntervals){
		String [] parts = strIntervals.split("/");
		DateTime start = INPUT_DRUID_TIME_FORMATTER.parseDateTime(parts[0]);
		DateTime end = INPUT_DRUID_TIME_FORMATTER.parseDateTime(parts[1]);
		DateRange range = new DateRange(start, end);
		return range;
	}
}
