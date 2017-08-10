package com.maplecloudy.distribute.engine.app.jetty;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import com.maplecloudy.distribute.engine.appserver.AppPara;

public class JettyPara extends AppPara {
  public static String APP_TYPE = "JETTY";
  
  public JettyPara() {
    this.isDistribution = false;
  }
  
  public String elasticsearchUrl = "";
  public String sc = "";
  public String warFile = "";
  public String getSc() {
    if (sc == null || "".equals(sc.trim())) {
      return "chmod 777 -R jetty-distribution-7.6.21.v20160908.zip/jetty-distribution-7.6.21.v20160908/*\n"
          + "cp rs-0.0.1-SNAPSHOT.war jetty-distribution-7.6.21.v20160908.zip/jetty-distribution-7.6.21.v20160908/webapps/root.war\n"
          + "rm jetty-distribution-7.6.21.v20160908.zip/jetty-distribution-7.6.21.v20160908/etc/jetty.xml\n"
          + "cp jetty.xml jetty-distribution-7.6.21.v20160908.zip/jetty-distribution-7.6.21.v20160908/etc\n"
          + "jetty-distribution-7.6.21.v20160908.zip/jetty-distribution-7.6.21.v20160908/bin/jetty.sh start";
    } else return sc;
  }
  public String getWarFile(){
    return this.warFile;
  }
  
  public static final String elasticsearchUrlLine = "#elasticsearch.url: \"\"";
  public static final String serverPort = "#setPort";
  
  public String getConfFile() {
    return this.user + "/" + this.project + "/" + this.appConf + "/"
        + this.appId + "/jetty.xml";
  }
  
  public String getScFile() {
    return this.user + "/" + this.project + "/" + this.appConf + "/"
        + this.appId + "/jetty.sh";
  }
  
  public String GenerateSc() throws Exception {
    File cf = new File(getScFile());
    new File(cf.getParent()).mkdirs();
    PrintWriter printWriter = new PrintWriter(getScFile());
    printWriter.append(this.getSc());
    printWriter.flush();
    printWriter.close();
    return getScFile();
  }
  
  public String GenerateConf(int port) throws Exception {
    File cf = new File(getConfFile());
    new File(cf.getParent()).mkdirs();
    PrintWriter printWriter = new PrintWriter(getConfFile());
    BufferedReader bufReader = new BufferedReader(new InputStreamReader(
        this.getClass().getResourceAsStream("jetty.xml")));
    for (String temp = null; (temp = bufReader
        .readLine()) != null; temp = null) {
//      if (elasticsearchUrlLine.equals(temp.trim())) {
//        temp = "elasticsearch.url: " + elasticsearchUrl;
//      }
      if (serverPort.equals(temp.trim())) {
        temp = "<Set name=\"port\"><Property name=\"jetty.port\" default=\""+port+"\"/></Set>";  
      }
      printWriter.append(temp);
      printWriter.append(System.getProperty("line.separator"));// 行与行之间的分割
      
    }
    printWriter.flush();
    printWriter.close();
    return getConfFile();
  }
  
  public static int main(String[] args) {
    return 0;
  }
  
  @Override
  public String getName() {
    return this.user + "|" + this.project + "|" + this.appConf + "|"
        + this.appId;
  }
  
  @Override
  public String getAppType() {
    
    return APP_TYPE;
  }
}
