package com.maplecloudy.bi.interceptor;

import java.util.List;

import com.google.common.collect.Lists;
import com.maplecloudy.bi.model.TimeRecord;

/**
 * Simple Interceptor class that sets the current system timestamp on all events
 * that are intercepted. By convention, this timestamp header is named
 * "timestamp" and its format is a "stringified" long timestamp in milliseconds
 * since the UNIX epoch.
 */
public class TimestampInterceptor implements Interceptor<TimeRecord> {
  
  private final long startTime;
  private final long endTime;
  
  /**
   * Only {@link TimestampInterceptor.Builder} can build me
   */
  public TimestampInterceptor(long startTime, long endTime) {
    this.startTime = startTime;
    this.endTime = endTime;
  }
  
  @Override
  public void initialize() {
    // no-op
  }
  
  /**
   * Modifies log in-place.
   */
  @Override
  public TimeRecord intercept(TimeRecord log) {
    if (endTime > log.timestamp && log.timestamp >= startTime) return log;
    else return null;
  }
  
  /**
   * Delegates to {@link #intercept(AdLog)} in a loop.
   * 
   * @param logs
   * @return
   */
  @Override
  public List<TimeRecord> intercept(List<TimeRecord> logs) {
    List<TimeRecord> out = Lists.newArrayList();
    for (TimeRecord log : logs) {
      TimeRecord outLog = intercept(log);
      if (outLog != null) {
        out.add(outLog);
      }
    }
    return out;
  }
}
