package com.maplecloudy.source.report;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;
import com.maplecloudy.bi.ReportConstants;
import com.maplecloudy.bi.ReportFrequence;
import com.maplecloudy.bi.util.BucketPath;
import com.maplecloudy.bi.util.ReportConstantsUtils;
import com.maplecloudy.source.Source;

public class IntermediateSource implements Source {
//  public final static String INTERMEDIATE_KEY_OUTPUT = "/quickly-report/intermediate-key/";
	public final static String INTERMEDIATE_KEY_OUTPUT = ReportConstants.INTERMEDIATE_KEY_OUTPUT;
  
  private IntermediateSource() {
    
  }
  
  private static IntermediateSource ids = new IntermediateSource();
  
  public static IntermediateSource get() {
    return ids;
  }
  
  public Path getOutput(final Date startDate, ReportFrequence frequence) {
    Path out = null;
    Path base = new Path(INTERMEDIATE_KEY_OUTPUT, frequence.name());
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
    	  out = new Path(base, BucketPath.escapeString("%Y-%m-%d", startDate));
          break;
      case Monthly:
        out = new Path(base, BucketPath.escapeString("%Y-%m", startDate));
        break;
    }
    return out;
  }
  
  public Iterable<Path> getInputs(final Date reportTime,
      ReportFrequence frequence, Class<?>[] ras) {
    return getInputs(reportTime, frequence, Arrays.asList(ras));
  }
  
  public Iterable<Path> getInputs(final Date reportTime,
      ReportFrequence frequence, List<Class<?>> ras) {
    List<Path> lst = Lists.newArrayList();
    
    Path base = getOutput(reportTime, frequence);
    // Path current = ConstantsUtils.getCurrentDir(base);
    for (Class<?> ra : ras) {
      Path pra = new Path(base, ra.getSimpleName());
      lst.add(ReportConstantsUtils.getCurrentDir(pra));
    }
    return lst;
  }
  
  public Iterable<Path> getInputs(final Date startDate, final Date endDate,
      ReportFrequence frequence, Class<?>[] ras) {
	  return getInputs(startDate, endDate, frequence, Arrays.asList(ras));
  }
  
  public Iterable<Path> getInputs(final Date startDate, final Date endDate,
      ReportFrequence frequence, List<Class<?>> ras) {
    List<Path> lst = Lists.newArrayList();
    final Calendar start = Calendar.getInstance();
    start.setTime(startDate);
    Calendar end = Calendar.getInstance();
    end.setTime(endDate);
    
    while (start.before(end)) {
      Path base = getOutput(start.getTime(), frequence);
      for (Class<?> ra : ras) {
        // modify by zyb
        Path pra = ReportConstantsUtils.getCurrentDir(new Path(base, ra
            .getSimpleName()));
        // Path pra = new Path(ConstantsUtils.getCurrentDir(base),
        // ra.getSimpleName());
        lst.add(pra);
      }
      nextTime(start, frequence);
    }
    return lst;
  }
  
  public void nextTime(Calendar now, ReportFrequence frequence) {
    switch (frequence) {
      case NONE:
      case Hourly:
        now.add(Calendar.HOUR, 1);
        break;
      case Daily:
        now.add(Calendar.DATE, 1);
        break;
      case Weekly:
        now.add(Calendar.DATE, 7);
        break;
      case Thirtydays:
    	  now.add(Calendar.DATE, 30);
          break; 
      case Monthly:
        now.add(Calendar.MONTH, 1);
        break;
    }
  }
}
