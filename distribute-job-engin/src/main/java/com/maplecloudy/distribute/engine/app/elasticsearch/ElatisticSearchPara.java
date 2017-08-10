package com.maplecloudy.distribute.engine.app.elasticsearch;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;

import com.maplecloudy.distribute.engine.appserver.AppPara;

public class ElatisticSearchPara extends AppPara {
  public static String APP_TYPE = "ELATISTICSEARCH";
  public String dataPath;
  public String logPath;
  public String networkHost;
  public int amMem = 64;
  public int amVCores = 1;
  public int containers = 1;
  public final static String clusterNameLine = "#cluster.name: my-application";
  public final static String dataPathLine = "path.data: /tmp/elasticsearch/data";
  public final static String logPathLine = "path.logs: /tmp/elasticsearch/logs";
  public final static String networkHostLine = "network.host: 0.0.0.0";
  
  public ElatisticSearchPara() {
    this.isDistribution = false;
  }
  
  public String getConfFile() {
    return this.user + "/" + this.project + "/" + this.appConf + "/"
        + this.appId + "/elatisticsearch.yml";
  }
  
  public String getJvmOptionFile() {
    return this.user + "/" + this.project + "/" + this.appConf + "/"
        + this.appId + "/jvm.options";
  }
  
  public String getScFile() {
    return this.user + "/" + this.project + "/" + this.appConf + "/"
        + this.appId + "/elatisticsearch.sh";
  }
  
  public String GenerateConf() throws Exception {
    File cf = new File(getConfFile());
    new File(cf.getParent()).mkdirs();
    PrintWriter printWriter = new PrintWriter(getConfFile());
    BufferedReader bufReader = new BufferedReader(new InputStreamReader(this
        .getClass().getResourceAsStream("elasticsearch.yml")));
    for (String temp = null; (temp = bufReader.readLine()) != null; temp = null) {
      if (clusterNameLine.equals(temp.trim())) {
        temp = "cluster.name: " + this.getName();
      } else if (dataPathLine.equals(temp.trim()) && dataPath != null
          && !dataPath.trim().isEmpty()) {
        temp = "spath.data: " + dataPath;
      } else if (logPath.equals(temp.trim()) && logPath != null
          && !logPath.trim().isEmpty()) {
        temp = "log.data: " + logPath;
      } else if (networkHostLine.equals(temp.trim()) && networkHost != null
          && !networkHost.trim().isEmpty()) {
        temp = "network.host: " + networkHost;
      }
      printWriter.append(temp);
      printWriter.append(System.getProperty("line.separator"));// 行与行之间的分割
      
    }
    printWriter.flush();
    printWriter.close();
    return getConfFile();
  }
  
  public String GenerateJvmOptions() throws Exception {
    File cf = new File(this.getJvmOptionFile());
    new File(cf.getParent()).mkdirs();
    PrintWriter printWriter = new PrintWriter(getConfFile());
    BufferedReader bufReader = new BufferedReader(new InputStreamReader(this
        .getClass().getResourceAsStream("jvm.options")));
    for (String temp = null; (temp = bufReader.readLine()) != null; temp = null) {
      if ("-Xms1024m".equals(temp.trim()) ) {
        temp = "-Xms" +this.memory+"m";
      }else if("-Xmx1024m".equals(temp.trim())) {
        temp = "-Xmx" +this.memory+"m";
      }  
   
      printWriter.append(temp);
      printWriter.append(System.getProperty("line.separator"));// 行与行之间的分割
      
    }
    printWriter.flush();
    printWriter.close();
    return this.getJvmOptionFile();
  }
  
  public static int main(String[] args) {
    return 0;
  }
  
  @Override
  public String getName() {
    return this.user + "|" + this.project + "|" + this.appConf + "|"
        + this.appId;
  }
  
  public void setParatoCfg(Configuration conf)
  {
    conf.set("elatistic.search.para.user", this.user);
    
  }
  
  public static ElatisticSearchPara getFromCfg(Configuration conf)
  {
    ElatisticSearchPara para = new ElatisticSearchPara();
//    conf.set("elatistic.search.para.user", );
    return para;
  }
  public String getJavaHome()
  {
    return ElasticsearchInstallInfo.getJavaHome();
  }
  @Override
  public String getAppType() {
    
    return APP_TYPE;
  }

  public String[] extArcs() {
    return null;
  }
}
