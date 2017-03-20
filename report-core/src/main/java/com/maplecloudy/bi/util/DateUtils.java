/**
 * @copyright 北京力美  Lmmob
 */
package com.maplecloudy.bi.util;

import java.text.NumberFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * 
 * @author hanyuanlei
 * @ClassName DateUtils
 * @Description:TODO
 * @since 2011-12-27 下午09:01:52
 * @version V1.0
 */
public class DateUtils {
	/**
	 * 
	 * @param date
	 * @param day
	 * @param fmtString
	 * @return 当前日期-至-减去/加上day天之间的日期
	 */
	public static String[] getCurrDiffDayBetween(Date date, int day,
			String fmtString) {
		String[] arr = null ;
		if( day < 0 ){
			arr = new String[-day];
			for (int i = 0; i > day; i--) {
				arr[-i] = dateFmt(diffDays(date, i-1),fmtString);
			}
		}else if (day==0){
			arr = new String[]{dateFmt( date,fmtString)};
		}else {
			arr = new String[day];
			for (int i = 0; i < day; i++) {
				arr[i] = dateFmt(diffDays(date, i+1),fmtString);
			}
		}
		
		
		return arr;
	}
	
	
	/**
	 * 根据格式化字符串输出
	 * 
	 * @param date
	 * @param fmtString
	 *            如 yyyyMMddHHmm 等
	 * @return 一定格式的 日期
	 * @Exception
	 */
	public static String dateFmt(Date date, String fmtString) {
		if (null == date)
			return "";
		try {
			String str = "";
			SimpleDateFormat format = new SimpleDateFormat(fmtString);
			str = format.format(date);
			return str;
		} catch (Exception e) {
			return date.toString();
		}
	}

	/**
	 * 
	 * @Title: diffHours
	 * @Description: 计算 小时
	 * @param @param date
	 * @param @param hour 正数 加上，负数 减去
	 * @param @return
	 * @return Date
	 * @throws
	 */
	public static Date diffHours(Date date, int hour) {
		java.util.Calendar c = java.util.Calendar.getInstance();
		c.setTimeInMillis(getMillis(date) + ((long) hour) * 3600 * 1000);
		return c.getTime();
	}

	/**
	 * 解析字符 为 日期
	 * 
	 * @Title: strToDate
	 * @Description: TODO
	 * @param @param strDate 字符型日期
	 * @param @param template 日期模版
	 * @param @return
	 * @return Date
	 * @throws
	 */
	public static Date strToDate(String strDate, String template) {
		SimpleDateFormat formatter = new SimpleDateFormat(template);
		ParsePosition pos = new ParsePosition(0);
		Date strtodate = formatter.parse(strDate, pos);
		return strtodate;
	}

	/**
	 * 把数字带上千分位
	 */

	public static String toString3num(Double num) {
		NumberFormat numfmt = NumberFormat.getInstance();
		String str = numfmt.format(num);
		return str;

	}

	// 把2010-10-20T00:00:00格式的截取为2010-10-20
	public static String parseTimeMap(String time) {
		// 2010-10-20
		// System.out.println(time.substring(0, 10));
		time = time == null ? "" : time.substring(0, 10);
		return time;
	}

	public static long getMillis(java.util.Date date) {
		java.util.Calendar c = java.util.Calendar.getInstance();
		c.setTime(date);
		return c.getTimeInMillis();
	}

	// 日期转化为大写各式

	// 日期转化为大小写
	public static String dataToUpper(Date date) {
		Calendar ca = Calendar.getInstance();
		ca.setTime(date);
		int year = ca.get(Calendar.YEAR);
		int month = ca.get(Calendar.MONTH) + 1;
		int day = ca.get(Calendar.DAY_OF_MONTH);
		return numToUpper(year) + "年" + monthToUppder(month) + "月"
				+ dayToUppder(day) + "日";
	}

	// 将数字转化为大写
	public static String numToUpper(int num) {
		// String u[] = {"零","壹","贰","叁","肆","伍","陆","柒","捌","玖"};
		String u[] = { "0", "一", "二", "三", "四", "五", "六", "七", "八", "九" };
		char[] str = String.valueOf(num).toCharArray();
		String rstr = "";
		for (int i = 0; i < str.length; i++) {
			rstr = rstr + u[Integer.parseInt(str[i] + "")];
		}
		return rstr;
	}

	// 月转化为大写
	public static String monthToUppder(int month) {
		if (month < 10) {
			return numToUpper(month);
		} else if (month == 10) {
			return "十";
		} else {
			return "十" + numToUpper(month - 10);
		}
	}

	// 日转化为大写
	public static String dayToUppder(int day) {
		if (day < 20) {
			return monthToUppder(day);
		} else {
			char[] str = String.valueOf(day).toCharArray();
			if (str[1] == '0') {
				return numToUpper(Integer.parseInt(str[0] + "")) + "十";
			} else {
				return numToUpper(Integer.parseInt(str[0] + "")) + "十"
						+ numToUpper(Integer.parseInt(str[1] + ""));
			}
		}
	}

	// ----------------------------------------

	/**
	 * 得到一个日期前多少天的那天的日期
	 * 
	 * @param date
	 *            日期
	 * @param day
	 *            天数
	 * @return 返回相减后的日期
	 */
	public static Date diffDays(java.util.Date date, int day) {
		java.util.Calendar c = java.util.Calendar.getInstance();
		c.setTimeInMillis(getMillis(date) + ((long) day) * 24 * 3600 * 1000);
		return c.getTime();
	}

	/**
	 * 
	 * @Title: getTableDayHourName
	 * @Description: TODO 返回某天24小时的字符串 集合 如：2012020301、2012-02-0301
	 * @param @param date
	 * @param @param fmtString
	 * @param @return
	 * @return List<String>
	 * @throws
	 */
	public static List<String> getTableDayHourName(Date date, String fmtString) {
		String str = "";
		if (null == date)
			date = new Date();
		try {
			SimpleDateFormat format = new SimpleDateFormat(fmtString);
			str = format.format(date);
		} catch (Exception e) {
		}
		List<String> tableNames = new ArrayList<String>();
		String tableDateName = str;
		for (int i = 0; i < 24; i++) {
			String hour = "";
			if (i < 10)
				hour = "0" + i;
			else
				hour = String.valueOf(i);
			tableNames.add(tableDateName + hour);
		}

		return tableNames;

	}

	/**
	 * 
	 * @Title: getDayString
	 * @Description: TODO返回两个日期之间的天（如：20120303、20120304）
	 * @param @param startDate
	 * @param @param endDate
	 * @param @return
	 * @return int[]
	 * @throws
	 */
	public static int[] getDayString(String startDate, String endDate) {
		int startNum = Integer.valueOf(startDate);
		int endNum = Integer.valueOf(endDate);
		int[] arr = new int[endNum - startNum];
		for (int i = startNum; i < endNum; i++) {
			arr[i - startNum] = i;
		}
		return arr;
	}

	/**
	 * 
	 * @Title: getTableDayHourName
	 * @Description: TODO 返回某天24小时的字符串 集合 如：2012020301、2012-02-03 01
	 * @param @param date
	 * @param @param fmtString
	 * @param @return
	 * @return List<String>
	 * @throws
	 */
	public static List<String> getTableDayHourName(String startDate,
			String endDate) {
		// 两个日期之间的日期串
		int[] dateStrings = getDayString(startDate, endDate);

		List<String> tableNames = new ArrayList<String>();
		for (int date : dateStrings) {
			for (int i = 0; i < 24; i++) {
				String hour = "";
				if (i < 10)
					hour = "0" + i;
				else
					hour = String.valueOf(i);
				tableNames.add(date + hour);
			}
		}
		return tableNames;

	}

}
