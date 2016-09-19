package com.maplecloudy.bi.interceptor;

import java.util.List;

public interface Interceptor<K> {
  /**
   * Any initialization / startup needed by the Interceptor.
   */
  public void initialize();
  
  /**
   * Interception of a single {@link K}.
   * 
   * @param log
   *          LogBase to be intercepted
   * @return Original or modified log, or {@code null} if the Event is to be
   *         dropped (i.e. filtered out).
   */
  public K intercept(K log);
  
  /**
   * Interception of a batch of {@linkplain K }.
   * 
   * @param events
   *          Input list of K
   * @return Output list of logs. The size of output list MUST NOT BE GREATER
   *         than the size of the input list (i.e. transformation and removal
   *         ONLY). Also, this method MUST NOT return {@code null}. If all logs
   *         are dropped, then an empty List is returned.
   */
  public List<K> intercept(List<K> logs);
  
}
