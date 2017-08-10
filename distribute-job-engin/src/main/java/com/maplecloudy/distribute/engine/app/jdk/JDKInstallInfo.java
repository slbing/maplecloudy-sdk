package com.maplecloudy.distribute.engine.app.jdk;

public class JDKInstallInfo {
  public String user = "maplecloudy";//install this app's user
  public String intallPath = "com/java/jdk/";
  public String version = "8u121";
  public String pack = "jdk-8u121-linux-x64.tar.gz";
  private static JDKInstallInfo kii = new JDKInstallInfo();
  public static JDKInstallInfo getInstance()
  {
    return kii;
  }
  public static String getPack()
  {
    return "/user/"+kii.user+"/"+kii.intallPath+"/"+kii.version+"/"+kii.pack;
  }
}
