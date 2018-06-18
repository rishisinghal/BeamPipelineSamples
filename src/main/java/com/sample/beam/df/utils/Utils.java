package com.sample.beam.df.utils;

import java.text.SimpleDateFormat;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class Utils {
	
	public static DateTimeFormatter dateMsFormatter = DateTimeFormat.forPattern("YYYY-MM-dd'T'HH:mm:ss.SSS");	
	public static SimpleDateFormat dateSecFormatter = new SimpleDateFormat("YYYY-MM-dd-HH-mm-ss");

}
