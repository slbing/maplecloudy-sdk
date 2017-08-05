package com.maplecloudy.distribute.engine.app.elasticsearch;

public class ElasticsearchInstallInfo {
  public String user = "maplecloudy";//install this app's user
  public String intallPath = "com/elasticsearch/kibana/";
  public String version = "5.3.0";
  public String pack = "kibana-5.3.0-linux-x86_64.zip";
  private static ElasticsearchInstallInfo kii = new ElasticsearchInstallInfo();
  public static String getPack()
  {
    return "/user/"+kii.user+"/"+kii.intallPath+"/"+kii.version+"/"+kii.pack;
  }
  
}
