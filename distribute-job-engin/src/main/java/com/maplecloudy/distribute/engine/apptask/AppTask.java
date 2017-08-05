package com.maplecloudy.distribute.engine.apptask;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.codehaus.jackson.map.util.Comparators;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.maplecloudy.distribute.engine.app.engine.EngineInstallInfo;
import com.maplecloudy.distribute.engine.appserver.AppPara;
import com.maplecloudy.distribute.engine.appserver.AppStatus;
import com.maplecloudy.distribute.engine.utils.EngineUtils;

public abstract class AppTask extends Configured implements Runnable {
  public int port = 0;
  public static final String DEFAULT_INSTALL_USER = "maplecloudy";
  public AppPara para;
  public List<ApplicationId> appids = Lists.newArrayList();
  
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
  
  public List<AppStatus> getAppStatus() throws Exception {
    List<AppStatus> las = Lists.newArrayList();
    this.appids.clear();
    YarnClient yarnClient = YarnClient.createYarnClient();
    Configuration conf = new YarnConfiguration(this.getConf());
    yarnClient.init(conf);
    yarnClient.start();
    List<ApplicationReport> reports = yarnClient.getApplications(Collections
        .singleton(this.para.getAppType()));
    for (ApplicationReport report : reports) {
      if (report.getName().equals(this.getName())) {
        AppStatus as = new AppStatus();
        as.appStatus = report.getYarnApplicationState().name();
        as.appid = report.getApplicationId().toString();
        as.diagnostics = report.getDiagnostics();
        as.host = report.getHost();
        as.port = this.getPort();
        checkInfo.add("App has run with appid:" + report.getApplicationId());
        this.appids.add(report.getApplicationId());
        las.add(as);
        Collections.sort(las, new Comparator<AppStatus>() {
          
          @Override
          public int compare(AppStatus o1, AppStatus o2) {
            
            // application_1489135095681_0017
            String[] ids1 = o1.appid.split("_");
            ApplicationId appid1 = ApplicationId.newInstance(
                Long.parseLong(ids1[1]), Integer.parseInt(ids1[2]));
            String[] ids2 = o2.appid.split("_");
            ApplicationId appid2 = ApplicationId.newInstance(
                Long.parseLong(ids2[1]), Integer.parseInt(ids2[2]));
            return appid2.compareTo(appid1);
          }
          
        });
      }
    }
    
    return las;
  }
  
  public int stopApp() throws Exception {
    
    YarnClient yarnClient = YarnClient.createYarnClient();
    Configuration conf = new YarnConfiguration(this.getConf());
    yarnClient.init(conf);
    yarnClient.start();
    List<ApplicationReport> reports = yarnClient.getApplications(Collections
        .singleton(this.para.getAppType()));
    
    // List<ApplicationReport> reports = yarnClient.getApplications(Collections
    // .singleton("MAPLECLOUDY-APP"));
    int bret = -1;
    for (ApplicationReport report : reports) {
      if (report.getName().equals(this.getName())) {
        if (report.getYarnApplicationState() != YarnApplicationState.FAILED
            && report.getYarnApplicationState() != YarnApplicationState.FINISHED
            && report.getYarnApplicationState() != YarnApplicationState.KILLED) {
          checkInfo.add("Stop app as status:" + report.getApplicationId());
          yarnClient.killApplication(report.getApplicationId());
          bret = 0;
        }
      }
    }
    return bret;
  }
  
  public boolean checkTaskApp() throws YarnException, IOException {
    boolean bret = false;
    YarnClient yarnClient = YarnClient.createYarnClient();
    Configuration conf = new YarnConfiguration(this.getConf());
    yarnClient.init(conf);
    yarnClient.start();
    List<ApplicationReport> reports = yarnClient.getApplications(Collections
        .singleton(this.para.getAppType()));
    for (ApplicationReport report : reports) {
      if (report.getName().equals(this.getName())) {
        checkInfo.add("App has run with appid:" + report.getApplicationId());
        
        if (report.getYarnApplicationState() == YarnApplicationState.FAILED
            || report.getYarnApplicationState() == YarnApplicationState.FINISHED
            || report.getYarnApplicationState() == YarnApplicationState.KILLED) {
          checkInfo.add("App have run with appid:" + report.getApplicationId()
              + ", and not runing with status:"
              + report.getYarnApplicationState());
        } else {
          this.appids.add(report.getApplicationId());
          bret = true;
        }
      }
      
    }
    yarnClient.close();
    return bret;
  }
  
  public static class RunPara {
    public int port = 0;
  }
  
  public int getPort() throws IOException {
    String runParaFile = this.para.user + "/" + this.para.project + "/"
        + this.para.appConf + "/" + this.para.appId + "/run.para";
    File rpf = new File(runParaFile);
    
    Gson gson = new GsonBuilder().create();
    if (rpf.exists()) {
      RunPara rp = gson.fromJson(new FileReader(runParaFile), RunPara.class);
      if (rp == null || rp.port == 0) {
        this.port = EngineUtils.getRandomPort();
        rp.port = port;
        this.saveRunPara(rp, rpf);
      } else this.port = rp.port;
    } else if (this.port == 0) {
      this.port = EngineUtils.getRandomPort();
      RunPara rp = new RunPara();
      rp.port = port;
      this.saveRunPara(rp, rpf);
      
    }
    return this.port;
  }
  
  public void saveRunPara(RunPara rp, File rpf) throws JsonIOException,
      IOException {
    if (!rpf.exists()) {
      new File(rpf.getParent()).mkdirs();
    }
    Gson gson = new GsonBuilder().create();
    FileWriter wr = new FileWriter(rpf);
    gson.toJson(rp, wr);
    wr.flush();
    wr.close();
  }
}
