package com.maplecloudy.index.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.junit.Test;

public class TestFormatDate {
	@Test
	public void testDate2Long() throws ParseException{
		String s = "2014-01-15 09:09:07 535";
		SimpleDateFormat sdf  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		System.out.println(sdf.parse(s).getTime());
		
	}
}
