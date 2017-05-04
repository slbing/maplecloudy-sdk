package com.maplecloudy.distribute.engine.utils;

import java.io.IOException;
import java.util.Properties;

public class Config {
  
  private static final String CLIENT_CFG = "/cfg.properties";
  private static  Properties cfg = new Properties();
  static
  {
    try {
      cfg.load(Config.class.getResourceAsStream(CLIENT_CFG));
    } catch (IOException e) {
      e.printStackTrace();
    }  
  }
  
  public static String getEngieJar() {
    if (cfg != null) {
      return cfg.getProperty("hdfs.engine.jar", "/user/maplecloudy");
    }
    return "/user/maplecloudy";
  }
  
}
