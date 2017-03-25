package com.maplecloudy.distribute.engine;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.google.inject.ImplementedBy;


/**
 * Context interface for sharing information across components in YARN App.
 */
@InterfaceAudience.Private
@ImplementedBy(AppContextImpl.class)
public interface AppContext {

  ApplicationId getApplicationID();

  ApplicationAttemptId getApplicationAttemptId();

  String getApplicationName();

  long getStartTime();

  CharSequence getUser();
  
  String getNMHostname();

  
}
