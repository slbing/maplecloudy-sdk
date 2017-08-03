package com.maplecloudy.distribute.engine.app.kibana;

public class KibanaInstallInfo {
  public String user = "maplecloudy";
  public String intallPath = "com/elasticsearch/kibana/";
  public String version = "5.3.0";
  public String pack = "kibana-5.3.0-linux-x86_64.zip";
  private static KibanaInstallInfo kii = new KibanaInstallInfo();
  public static String getPack()
  {
    return "/user/"+kii.user+"/"+kii.intallPath+"/"+kii.version+"/"+kii.pack;
  }
  
}
