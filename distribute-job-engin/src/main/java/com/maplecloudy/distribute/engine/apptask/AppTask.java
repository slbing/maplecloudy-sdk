package com.maplecloudy.distribute.engine.apptask;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.maplecloudy.distribute.engine.app.engine.EngineInstallInfo;
import com.maplecloudy.distribute.engine.appserver.AppPara;
import com.maplecloudy.distribute.engine.appserver.AppStatus;

public abstract class AppTask extends Configured implements Runnable {
  public AppPara para;
  public ApplicationId appid = null;
  
  public AppTask(AppPara para) {
    this.para = para;
    Configuration conf = new Configuration();
    this.para.setConf(conf);
    this.setConf(conf);
  }
  
  public abstract String getName();
  
  public List<String> checkInfo = new ArrayList<String>();
  
  public boolean checkEngine() throws IOException {
    boolean bret = true;
    final FileSystem fs = FileSystem.get(getConf());
    if (fs.exists(new Path(EngineInstallInfo.getPack()))) {
      checkInfo.add("Maplecloudy Engine install ok!");
    } else {
      checkInfo.add("Try install Maplecloudy engine!");
      UserGroupInformation ugi = UserGroupInformation.createProxyUser(
          "maplecloudy", UserGroupInformation.getLoginUser());
      bret = ugi.doAs(new PrivilegedAction<Boolean>() {
        @Override
        public Boolean run() {
          try {
            fs.copyFromLocalFile(false, true, new Path("lib/"
                + EngineInstallInfo.pack),
                new Path(EngineInstallInfo.getPack()));
          } catch (IllegalArgumentException | IOException e) {
            e.printStackTrace();
            checkInfo.add("Install Maplecloudy engine faild whit:"
                + e.getMessage());
            return false;
          }
          checkInfo.add("Install Maplecloudy engine succes!");
          return true;
        }
        
      });
    }
    return bret;
  }
  
  public AppStatus getAppStatus() throws Exception {
    AppStatus as = new AppStatus();
    if (this.appid == null) as.error = "this app not runing";
    else {
      YarnClient yarnClient = YarnClient.createYarnClient();
      Configuration conf = new YarnConfiguration(this.getConf());
      yarnClient.init(conf);
      yarnClient.start();
      ApplicationReport report = yarnClient.getApplicationReport(this.appid);
      as.appStatus = report.getFinalApplicationStatus().name();
      as.appid = this.appid.toString();
      yarnClient.close();
    }
    return as;
  }
  
  public boolean checkTaskApp() throws YarnException, IOException {
    boolean bret = false;
    YarnClient yarnClient = YarnClient.createYarnClient();
    Configuration conf = new YarnConfiguration(this.getConf());
    yarnClient.init(conf);
    yarnClient.start();
    List<ApplicationReport> reports = yarnClient.getApplications(Collections
        .singleton("MAPLECLOUDY-APP"));
    for (ApplicationReport report : reports) {
      if (report.getName().equals(this.getName())) {
        checkInfo.add("App has run with appid:" + report.getApplicationId());
        bret = true;
      }
      
    }
    yarnClient.close();
    return bret;
  }
}
