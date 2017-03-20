package com.maplecloudy.flume.channel;

/**
 * This interface represents a channel counter mbean. Any class implementing
 * this interface must sub-class
 * {@linkplain org.apache.flume.instrumentation.MonitoredCounterGroup}. This
 * interface might change between minor releases. Please see
 * {@linkplain org.apache.flume.instrumentation.ChannelCounter} class.
 */
public interface ChannelPlusCounterMBean {
  
  long getChannelSize();
  
  long getEventPutAttemptCount();
  
  long getEventTakeAttemptCount();
  
  long getEventPutSuccessCount();
  
  long getEventTakeSuccessCount();
  
  long getStartTime();
  
  long getStopTime();
  
  String getType();
}
