package com.maplecloudy.bi.util;

import java.util.Enumeration;

import javax.servlet.ServletContext;

import org.apache.hadoop.conf.Configuration;

public class ReportConfiguration {
  private final static String KEY = ReportConfiguration.class.getName();
  
  private ReportConfiguration() {} // singleton
  
  /** Create a {@link Configuration} for Report. */
  public static Configuration create(String resourceName) {
    Configuration conf = new Configuration();
    conf.addResource(resourceName);
    return conf;
  }
  
  /** Create a {@link Configuration} for Report. */
  public static Configuration create() {
    Configuration conf = new Configuration();
    conf.addResource("report.xml");
    return conf;
  }
  
  /**
   * Create a {@link Configuration} for Report front-end.
   * 
   * If a {@link Configuration} is found in the
   * {@link javax.servlet.ServletContext} it is simply returned, otherwise, a
   * new {@link Configuration} is created using the {@link #create()} method,
   * and then all the init parameters found in the
   * {@link javax.servlet.ServletContext} are added to the {@link Configuration}
   * (the created {@link Configuration} is then saved into the
   * {@link javax.servlet.ServletContext}).
   * 
   * @param application
   *          is the ServletContext whose init parameters must override those of
   *          Report.
   */
  public static Configuration get(ServletContext application) {
    Configuration conf = (Configuration) application.getAttribute(KEY);
    if (conf == null) {
      conf = create();
      Enumeration<?> e = application.getInitParameterNames();
      while (e.hasMoreElements()) {
        String name = (String) e.nextElement();
        conf.set(name, application.getInitParameter(name));
      }
      application.setAttribute(KEY, conf);
    }
    return conf;
  }
  
}
