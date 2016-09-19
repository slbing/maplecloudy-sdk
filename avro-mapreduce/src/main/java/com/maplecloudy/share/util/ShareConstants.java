package com.maplecloudy.share.util;


import java.text.SimpleDateFormat;
import java.util.regex.Pattern;

public interface ShareConstants {
  
  public final static int MD5_LENGTH = 16;
  public final static int SID_LENGTH = 32;
  public final static int LONG_LENGTH = 8;
  public final static String PATH_SPLIT_CHAR = "/";
  
  
  public final static String GROUP_UD_COUNTERS = "User-defined Counters";
  public final static String GROUP_EXCEPTION_COUNTERS = "Exception Counters";
  
  public final static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
  public final static String CURRENT = "current";
  public final static String STABLE = "stable";
  public final static String TMP = "tmp";
  public final static String OLD = "old";
  public final static String BAK = "bak";
  public final static SimpleDateFormat FORMAT_OOZIE = new SimpleDateFormat(
      "yyyy-MM-dd'T'HH:mm'Z'");
  
  public final static SimpleDateFormat FORMAT_SOLR = new SimpleDateFormat(
	      "yyyy-MM-dd'T'HH:mm:ss'Z'");
  public final static SimpleDateFormat FORMAT_NINJABEAT_USER_DATA_DIR = new SimpleDateFormat(
      "yyyyMMdd/HH");
  
  public final static SimpleDateFormat FORMAT_JOB_DATE = new SimpleDateFormat(
      "yyyyMMddHH");
  public static SimpleDateFormat FORMAT_MONTH = new SimpleDateFormat(
	      "yyyy-MM");
  public static SimpleDateFormat FORMAT_OUTPUT = new SimpleDateFormat(
      "yyyy-MM-dd/HH");
  public static SimpleDateFormat FORMAT_OUTPUT1 = new SimpleDateFormat(
      "yyyy-MM-dd/HH/mm");
  public static SimpleDateFormat FORMAT_DATE_OUTPUT = new SimpleDateFormat(
      "yyyy-MM-dd");
  
  public static SimpleDateFormat FORMAT_OUTPUT2 = new SimpleDateFormat("yyyy-MM-dd");

}