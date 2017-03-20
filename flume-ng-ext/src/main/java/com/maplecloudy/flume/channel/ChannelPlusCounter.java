package com.maplecloudy.flume.channel;

import org.apache.flume.instrumentation.MonitoredCounterGroup;

public class ChannelPlusCounter extends MonitoredCounterGroup implements
    ChannelPlusCounterMBean {

  private static final String COUNTER_CHANNEL_SIZE = "channel.current.size";

  private static final String COUNTER_EVENT_PUT_ATTEMPT =
      "channel.event.put.attempt";

  private static final String COUNTER_EVENT_TAKE_ATTEMPT =
      "channel.event.take.attempt";

  private static final String COUNTER_EVENT_PUT_SUCCESS =
      "channel.event.put.success";

  private static final String COUNTER_EVENT_TAKE_SUCCESS =
      "channel.event.take.success";

  

  private static final String[] ATTRIBUTES = {
    COUNTER_CHANNEL_SIZE, COUNTER_EVENT_PUT_ATTEMPT,
    COUNTER_EVENT_TAKE_ATTEMPT, COUNTER_EVENT_PUT_SUCCESS,
    COUNTER_EVENT_TAKE_SUCCESS, 
  };

  public ChannelPlusCounter(String name) {
    super(MonitoredCounterGroup.Type.CHANNEL, name, ATTRIBUTES);
  }

  @Override
  public long getChannelSize() {
    return get(COUNTER_CHANNEL_SIZE);
  }

  public void setChannelSize(long newSize) {
    set(COUNTER_CHANNEL_SIZE, newSize);
  }

  @Override
  public long getEventPutAttemptCount() {
    return get(COUNTER_EVENT_PUT_ATTEMPT);
  }

  public long incrementEventPutAttemptCount() {
    return increment(COUNTER_EVENT_PUT_ATTEMPT);
  }

  @Override
  public long getEventTakeAttemptCount() {
    return get(COUNTER_EVENT_TAKE_ATTEMPT);
  }

  public long incrementEventTakeAttemptCount() {
    return increment(COUNTER_EVENT_TAKE_ATTEMPT);
  }

  @Override
  public long getEventPutSuccessCount() {
    return get(COUNTER_EVENT_PUT_SUCCESS);
  }

  public long addToEventPutSuccessCount(long delta) {
    return addAndGet(COUNTER_EVENT_PUT_SUCCESS, delta);
  }

  @Override
  public long getEventTakeSuccessCount() {
    return get(COUNTER_EVENT_TAKE_SUCCESS);
  }

  public long addToEventTakeSuccessCount(long delta) {
    return addAndGet(COUNTER_EVENT_TAKE_SUCCESS, delta);
  }
  
}
