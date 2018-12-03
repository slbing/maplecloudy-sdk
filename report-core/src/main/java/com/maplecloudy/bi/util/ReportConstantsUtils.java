package com.maplecloudy.bi.util;

import java.util.Calendar;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

import com.maplecloudy.bi.ReportConstants;
import com.maplecloudy.bi.ReportFrequence;
import com.maplecloudy.bi.ReportProperties;

public class ReportConstantsUtils extends ConstantsUtils implements
    ReportConstants {
  private static final Log LOG = LogFactory.getLog(ReportConstantsUtils.class);
  
  public static void checkStartEndDate(final String startStr,
      final String endStr) throws Exception {
    final Date startDate = FORMAT_OOZIE.parse(startStr);
    final Date endDate = FORMAT_OOZIE.parse(endStr);
    if (startDate.after(endDate)) {
      final String msg = "start date=" + startStr + " is after end date="
          + endStr + ", It's wrong parameter.";
      LOG.error(msg);
      throw new Exception(msg);
    }
  }
  
  public static String[] getAlgorithms(ReportFrequence frequence) {
    return StringUtils.getTrimmedStrings(ReportProperties.commConf
        .getString(frequence.name().toLowerCase() + "."
            + ReportConstants.REPORT_ALGORITHMS));
  }
  
  public static Path getReportOutput(final Date startDate,
      ReportFrequence frequence) {
    Path out = null;
    Path base = new Path(REPORT_OUTPUT, frequence.name());
    switch (frequence) {
      case NONE:
      case Hourly:
        out = new Path(base, BucketPath.escapeString("%Y-%m-%d/%H", startDate));
        break;
      case Daily:
        out = new Path(base, BucketPath.escapeString("%Y-%m-%d", startDate));
        break;
      case Weekly:
        out = new Path(base, BucketPath.escapeString("%Y-%m-%d", startDate));
        break;
      case Thirtydays:
    	 out =  new Path(base, BucketPath.escapeString("%Y-%m-%d", startDate));
    	 break;
      case Monthly:
        out = new Path(base, BucketPath.escapeString("%Y-%m", startDate));
        break;
    }
    return out;
  }
  
  public static Path getSubflowOutput(final Date startDate,
      ReportFrequence frequence) {
    Path out = null;
    Path base = new Path(REPORT_OOZIE_SUBFLOW, frequence.name());
    switch (frequence) {
      case NONE:
      case Hourly:
        out = new Path(base, BucketPath.escapeString("%Y-%m-%d/%H", startDate));
        break;
      case Daily:
        out = new Path(base, BucketPath.escapeString("%Y-%m-%d", startDate));
        break;
      case Weekly:
        out = new Path(base, BucketPath.escapeString("%Y-%m-%d", startDate));
        break;
      case Thirtydays:
     	 out =  new Path(base, BucketPath.escapeString("%Y-%m-%d", startDate));
     	 break;
      case Monthly:
        out = new Path(base, BucketPath.escapeString("%Y-%m", startDate));
        break;
    }
    return out;
  }
  
  public static Date getEdndate(final Date startDate, ReportFrequence frequence) {
    Calendar cd = Calendar.getInstance();
    cd.setTime(startDate);
    switch (frequence) {
      case NONE:
      case Hourly:
        cd.add(Calendar.HOUR, 1);
        break;
      case Daily:
        cd.add(Calendar.DATE, 1);
        break;
      case Weekly:
        cd.add(Calendar.DATE, 7);
        break;
      case Thirtydays:
        cd.add(Calendar.DATE, 30);  
        break;
      case Monthly:
        cd.add(Calendar.MONTH, 1);
        break;
    }
    return cd.getTime();
  }
  
}
