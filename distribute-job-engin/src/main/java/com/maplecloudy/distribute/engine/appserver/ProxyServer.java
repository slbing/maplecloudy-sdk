package com.maplecloudy.distribute.engine.appserver;

public abstract class ProxyServer {
  public String name;// 用一个app的conf对应一个名字
  public final static String tmeplate = "#-----appname:<name>\n" + "server {\n"
      + " listen <proxyPort>;\n" + " access_log <logFile>;\n" + "\n"
      + " location / {\n" + " proxy_pass \"http://<appHost>:<appPort>\";\n"
      + " proxy_set_header Host $host:<appPort>;\n"
      + " proxy_set_header X-Real-IP $remote_addr;\n"
      + " proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;\n"
      + " proxy_set_header Via \"nginx\";\n" + " }\n" + "}\n\n";
  public abstract String getStoreContent();
}
