package com.maplecloudy.distribute.engine.app.elasticsearch;

import com.maplecloudy.distribute.engine.app.jdk.JDKInstallInfo;

public class ElasticsearchInstallInfo {
  public String user = "maplecloudy";//install this app's user
  public String intallPath = "com/elasticsearch/elasticsearch/";
  public String version = "5.3.0";
  public String pack = "elasticsearch-5.3.0.zip";
  public String shell = "elasticsearch-5.3.0/bin/elasticsearch";
  private static ElasticsearchInstallInfo kii = new ElasticsearchInstallInfo();
  public static ElasticsearchInstallInfo getInstance()
  {
    return kii;
  }
  
  public static String getPack()
  {
    return "/user/"+kii.user+"/"+kii.intallPath+"/"+kii.version+"/"+kii.pack;
  }
  public static String getJavaHome()
  {
    return JDKInstallInfo.getInstance().pack+"/jdk1.8.0_121";
  }
  public static String getJDK()
  {
    return JDKInstallInfo.getPack();
  }
}
