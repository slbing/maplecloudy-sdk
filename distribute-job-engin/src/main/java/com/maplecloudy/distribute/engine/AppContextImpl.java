package com.maplecloudy.distribute.engine;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;


/**
 * Context interface for sharing information across components in YARN App.
 */
@InterfaceAudience.Private
public class AppContextImpl implements AppContext{

  @Override
  public ApplicationId getApplicationID() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getApplicationName() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long getStartTime() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public CharSequence getUser() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getNMHostname() {
    // TODO Auto-generated method stub
    return null;
  }

  
}
