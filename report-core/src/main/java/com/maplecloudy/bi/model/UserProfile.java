package com.maplecloudy.bi.model;


public abstract class UserProfile<INPUT> {
  public long first_access_time = System.currentTimeMillis();
  public long last_access_time = Long.MIN_VALUE;
  
  public void merge(UserProfile<INPUT> lu) {
    this.first_access_time = Math.min(this.first_access_time,
        lu.first_access_time);
    this.last_access_time = Math
        .max(this.last_access_time, lu.last_access_time);
  }
  
  public abstract UserProfile<INPUT> getUserProfile(INPUT input);
}
