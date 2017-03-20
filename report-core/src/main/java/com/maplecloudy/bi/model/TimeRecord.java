package com.maplecloudy.bi.model;

public abstract class TimeRecord {
  public long timestamp;
  public abstract  TimeRecord getInstance(Object key,Object value);
}
