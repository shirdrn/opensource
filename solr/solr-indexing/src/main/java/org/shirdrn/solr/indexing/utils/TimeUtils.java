package org.shirdrn.solr.indexing.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TimeUtils {

	public static Date getDateBefore(int unit, int amount) {
		Calendar calendar = Calendar.getInstance();
		calendar.add(unit, amount);
		return calendar.getTime();
	}
	
	public static String format(Date date, String format) {
		DateFormat df = new SimpleDateFormat(format);
		return df.format(date);
	}
	
	public static String format(String date, String srcFormat, String dstFormat) {
		DateFormat df = new SimpleDateFormat(srcFormat);
		Date d = null;
		try {
			d = df.parse(date);
		} catch (ParseException e) {
			throw new RuntimeException(e);
		} finally {
			if(d != null) {
				df = new SimpleDateFormat(dstFormat);
			}
		}
		return df.format(d);
	}
	
	public static String format(long date, String format) {
		DateFormat df = new SimpleDateFormat(format);
		return df.format(new Date(date));
	}
}
