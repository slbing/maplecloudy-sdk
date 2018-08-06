package com.maplecloudy.distribute.engine.app.engine;


public class EngineInstallInfo {
  public static String user = "maplecloudy";
  public static String intallPath = "apps/maple-sdk/";
  public static String version = "0.3.0-SNAPSHOT";
  public static String pack = "distribute-job-engin-0.3.0-jdk8-SNAPSHOT.jar";
  public static String getPack()
  {
    return "/user/"+user+"/"+intallPath+"/"+version+"/"+pack;
  }
}
