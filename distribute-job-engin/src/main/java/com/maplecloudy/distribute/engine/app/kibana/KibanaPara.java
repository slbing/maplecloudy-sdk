package com.maplecloudy.distribute.engine.app.kibana;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import com.maplecloudy.distribute.engine.appserver.AppPara;

public class KibanaPara extends AppPara {
  public KibanaPara() {
    this.isDistribution = false;
  }
  
  public String elasticsearchUrl = "";
  public String sc = "";
  
  public String getSc() {
    if (sc == null || "".equals(sc.trim())) {
      return "chmod 777 -R kibana-5.3.0-linux-x86_64.zip/kibana-5.3.0-linux-x86_64/*\n"
          + "rm kibana-5.3.0-linux-x86_64.zip/kibana-5.3.0-linux-x86_64/config/kibana.yml\n"
          + "cp kibana.yml kibana-5.3.0-linux-x86_64.zip/kibana-5.3.0-linux-x86_64/config\n"
          + "kibana-5.3.0-linux-x86_64.zip/kibana-5.3.0-linux-x86_64/bin/kibana";
    } else return sc;
  }
  
  public static final String elasticsearchUrlLine = "#elasticsearch.url:";
  public static final String serverPort = "#server.port: 5601";
  
  public String getConfFile()
  {
    return this.user + "/" + this.project
        + "/" + this.appConf + "/" + this.appId + "/kibana.yml";
  }
  public String getScFile()
  {
    return this.user + "/" + this.project
        + "/" + this.appConf + "/" + this.appId + "/kibana.sh";
  }
  public String GenerateSc() throws Exception {
    PrintWriter printWriter = new PrintWriter(getScFile());
    printWriter.append(this.getSc());
    printWriter.close();
    return getScFile();
  }
  public String GenerateConf() throws Exception {
    File cf = new File(getConfFile());
    new File(cf.getParent()).mkdirs();
    PrintWriter printWriter = new PrintWriter(getConfFile());
    BufferedReader bufReader = new BufferedReader(new InputStreamReader(this
        .getClass().getResourceAsStream("kibana.yml")));
    for (String temp = null; (temp = bufReader.readLine()) != null; temp = null) {
      if (elasticsearchUrlLine.equals(temp.trim())) {
        temp = "elasticsearch.url:" + elasticsearchUrl;
      }
      if (serverPort.equals(temp.trim())) {
        temp = "server.port:" + this.port;
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
}
