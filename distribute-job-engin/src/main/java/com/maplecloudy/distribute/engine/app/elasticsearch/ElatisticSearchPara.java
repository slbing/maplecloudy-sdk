package com.maplecloudy.distribute.engine.app.elasticsearch;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import com.maplecloudy.distribute.engine.appserver.AppPara;

public class ElatisticSearchPara extends AppPara {
  public static String APP_TYPE = "ELATISTICSEARCH";
  public ElatisticSearchPara() {
    this.isDistribution = false;
  }
  
  public String getConfFile() {
    return this.user + "/" + this.project + "/" + this.appConf + "/"
        + this.appId + "/kibana.yml";
  }
  
  public String getScFile() {
    return this.user + "/" + this.project + "/" + this.appConf + "/"
        + this.appId + "/kibana.sh";
  }
  

  
  public String GenerateConf(int port) throws Exception {
    File cf = new File(getConfFile());
    new File(cf.getParent()).mkdirs();
    PrintWriter printWriter = new PrintWriter(getConfFile());
    BufferedReader bufReader = new BufferedReader(new InputStreamReader(this
        .getClass().getResourceAsStream("kibana.yml")));
 
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
