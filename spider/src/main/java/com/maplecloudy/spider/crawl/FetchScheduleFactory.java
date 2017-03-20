package com.maplecloudy.spider.crawl;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.maplecloudy.spider.util.ObjectCache;

/** Creates and caches a {@link FetchSchedule} implementation. */
public class FetchScheduleFactory {

  public static final Log LOG = LogFactory.getLog(FetchScheduleFactory.class);

  private FetchScheduleFactory() {}                   // no public ctor

  /** Return the FetchSchedule implementation. */
  public static FetchSchedule getFetchSchedule(Configuration conf) {
    String clazz = conf.get("db.fetch.schedule.class", DefaultFetchSchedule.class.getName());
    ObjectCache objectCache = ObjectCache.get(conf);
    FetchSchedule impl = (FetchSchedule)objectCache.getObject(clazz);
    if (impl == null) {
      try {
        LOG.info("Using FetchSchedule impl: " + clazz);
        Class implClass = Class.forName(clazz);
        impl = (FetchSchedule)implClass.newInstance();
        impl.setConf(conf);
        objectCache.setObject(clazz, impl);
      } catch (Exception e) {
        throw new RuntimeException("Couldn't create " + clazz, e);
      }
    }
    return impl;
  }
}
