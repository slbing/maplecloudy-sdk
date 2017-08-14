package com.maplecloudy.distribute.engine.app.jetty;

public class JettyInstallInfo {
  public String user = "maplecloudy";
  public String intallPath = "com/webserver/jetty";
  public String version = "7.6.21";
  public String pack = "jetty-distribution-7.6.21.v20160908.zip";
  private static JettyInstallInfo kii = new JettyInstallInfo();
  public static String getPack()
  {
    return "/user/"+kii.user+"/"+kii.intallPath+"/"+kii.version+"/"+kii.pack;
  }
}
