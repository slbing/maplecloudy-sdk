package com.maplecloudy.bi;

import com.maplecloudy.share.util.ShareConstants;

/**
 * Constants used in data files.
 */
public interface ReportConstants extends ShareConstants{

	public final static String REPORT_DIR = "quickly-report/";
  public final static String UUID_BLOOM_DIR = "quickly-report/bloom/uuid";
  
  public final static String USER_PROFILE_DB = "quickly-report/user-profile-db";
  public final static String REPORT_OUTPUT = "quickly-report/report/";
  public final static String INTERMEDIATE_KEY_OUTPUT = "quickly-report/intermediate-key/";
  public final static String FLUME_INPUT_FORMAT = "/%Y-%m-%d/%H";
  public final static String REPORT_OOZIE_SUBFLOW = "quickly-report/subflow";
  
  public final static String REPORT_TIME = "report.time";
  public final static String REPORT_FREQUENCE = "report.frequence";
  public final static String REPORT_APP_NAME = "report.appname";
  
  public final static String BLOOM_UUID = "bloom.uuid";
  public static final String REPORT_ALGORITHMS = "report.algorithms";
  
  public static final String REPORT_MODEL_NAME = "report.modelName"; 
  public static final String REPORT_TABLE_NAME = "report.tablename.current";
}
