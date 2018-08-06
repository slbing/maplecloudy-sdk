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
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.maplecloudy.distribute.engine.app.engine.EngineInstallInfo;
import com.maplecloudy.distribute.engine.appserver.AppStatus;
import com.maplecloudy.distribute.engine.utils.EngineUtils;
import com.maplecloudy.yarn.rpc.ClientRpc;

public abstract class AppTaskBaseline extends Configured implements Runnable {
  
  public static final String DEFAULT_INSTALL_USER = "maplecloudy";
  
  public List<ApplicationId> appids = Lists.newArrayList();
  
  public String user = "maplecloudy";
  
  public String project = "";
  
  public String appConf = "";
  // public int[] appId = {0};
  public int appId;
  public int memory = 1024;
  public int cpu = 1;
  
  // nginx para
  public String domain;
  public String nginxIp;
  public String nginxDomain;
  public int proxyPort = 0;
  public int port = 0;
  // @Nullable
  // public NgixGateway gateway;
  // public Cluster cluster;
  public String defaultFS = "";
  public String resourceManagerAddress = "1";
  
  public String type;
  
  public boolean isDistribution = true;
  
  public boolean damon = true;
  public boolean nginx = true;
  public JSONObject json;
  
  public AppTaskBaseline(JSONObject json) {
    
    try {
      this.project = json.getString("project");
      this.appConf = json.getString("appConf");
      this.appId = json.getInt("appId");
      this.memory = json.getInt("memory");
      this.cpu = json.getInt("cpu");
      this.domain = json.getString("domain");
      this.nginxIp = json.getString("nginxIp");
      this.nginxDomain = json.getString("nginxDomain");
      this.proxyPort = json.getInt("proxyPort");
      this.port = json.getInt("port");
      this.defaultFS = json.getString("defaultFS");
      this.resourceManagerAddress = json.getString("resourceManagerAddress");
      this.isDistribution = json.getBoolean("isDistribution");
      this.type = json.getString("type");
      this.user = json.getString("user");
      if (json.has("damon") && !json.getBoolean("damon")) this.damon = false;
      if (json.has("nginx") && !json.getBoolean("nginx")) this.nginx = false;
      this.json = json;
      exportJson(json);
      
    } catch (JSONException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    Configuration conf = new Configuration();
    // this.setTaskConf(conf);
    this.setConf(conf);
  }
  
  public List<String> runInfo = new ArrayList<String>();
  
  public boolean checkEngine() throws IOException {
    boolean bret = true;
    
    UserGroupInformation ugi = UserGroupInformation
        .createProxyUser("maplecloudy", UserGroupInformation.getLoginUser());
    bret = ugi.doAs(new PrivilegedAction<Boolean>() {
      @Override
      public Boolean run() {
        try {
          
          FileSystem fs = FileSystem.get(getConf());
          if (fs.exists(new Path(EngineInstallInfo.getPack()))) {
            runInfo.add("Maplecloudy Engine install ok!");
            return true;
          } else {
            runInfo.add("Try install Maplecloudy engine!");
            fs.copyFromLocalFile(false, true,
                new Path("lib/" + EngineInstallInfo.pack),
                new Path(EngineInstallInfo.getPack()));
            
            runInfo.add("Install Maplecloudy engine succes!");
            return true;
          }
        } catch (IllegalArgumentException | IOException e) {
          e.printStackTrace();
          runInfo
              .add("Install Maplecloudy engine faild whit:" + e.getMessage());
          return false;
        }
      }
      
    });
    return bret;
    
  }
  
  public List<AppStatus> getAppStatus() throws Exception {
    List<AppStatus> las = Lists.newArrayList();
    this.appids.clear();
    YarnClient yarnClient = YarnClient.createYarnClient();
    Configuration conf = new YarnConfiguration(this.getConf());
    yarnClient.init(conf);
    yarnClient.start();
    List<ApplicationReport> reports = yarnClient
        .getApplications(Collections.singleton(this.getAppType()));
    for (ApplicationReport report : reports) {
      if (report.getName().equals(this.getName())) {
        AppStatus as = new AppStatus();
        as.appStatus = report.getYarnApplicationState().name();
        as.appid = report.getApplicationId().toString();
        as.diagnostics = report.getDiagnostics();
        as.host = report.getHost();
        as.port = this.getPort();
        // checkInfo.add("App has run with appid:" + report.getApplicationId());
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
    List<ApplicationReport> reports = yarnClient
        .getApplications(Collections.singleton(this.getAppType()));
    
    // List<ApplicationReport> reports = yarnClient.getApplications(Collections
    // .singleton("MAPLECLOUDY-APP"));
    int bret = -1;
    for (ApplicationReport report : reports) {
      if (report.getName().equals(this.getName())) {
        if (report.getYarnApplicationState() != YarnApplicationState.FAILED
            && report.getYarnApplicationState() != YarnApplicationState.FINISHED
            && report
                .getYarnApplicationState() != YarnApplicationState.KILLED) {
          runInfo.add("Stop app as status:" + report.getApplicationId());
          yarnClient.killApplication(report.getApplicationId());
          bret = 0;
        }
      }
    }
    return bret;
  }
  
  public ApplicationId checkTaskApp() throws YarnException, IOException {
    ApplicationId bret = null;
    // YarnClient yarnClient = ClientRpc.getYarnClient(getConf());
    YarnClient yarnClient = YarnClient.createYarnClient();
    Configuration conf = new YarnConfiguration(this.getConf());
    yarnClient.init(conf);
    yarnClient.start();
    List<ApplicationReport> reports = yarnClient
        .getApplications(Collections.singleton(this.getAppType()));
    for (ApplicationReport report : reports) {
      if (report.getName().equals(this.getName())) {
        runInfo.add("App has run with appid:" + report.getApplicationId());
        if (report.getYarnApplicationState() == YarnApplicationState.FAILED
            || report.getYarnApplicationState() == YarnApplicationState.FINISHED
            || report
                .getYarnApplicationState() == YarnApplicationState.KILLED) {
          runInfo.add("App have run with appid:" + report.getApplicationId()
              + ", and not runing with status:"
              + report.getYarnApplicationState());
        } else {
          this.appids.add(report.getApplicationId());
          bret = report.getApplicationId();
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
    String runParaFile = this.user + "/" + this.project + "/" + this.appConf
        + "/" + this.appId + "/run.para";
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
  
  public void saveRunPara(RunPara rp, File rpf)
      throws JsonIOException, IOException {
    if (!rpf.exists()) {
      new File(rpf.getParent()).mkdirs();
    }
    Gson gson = new GsonBuilder().create();
    FileWriter wr = new FileWriter(rpf);
    gson.toJson(rp, wr);
    wr.flush();
    wr.close();
  }
  
  public String getName() {
    return this.user + "|" + this.project + "|" + this.appConf + "|"
        + this.appId;
  }
  
  public String getAppType() {
    return type;
  }
  
  // public void setTaskConf(Configuration conf) {
  // conf.set("yarn.resourcemanager.address", this.resourceManagerAddress);
  // conf.set("fs.defaultFS", this.defaultFS);
  //
  // conf.set("yarn.application.classpath",
  // "$HADOOP_CLIENT_CONF_DIR,$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*");
  //// conf.set("app.para", this.json.toString());
  // }
  
  public void exportJson(JSONObject json) throws IOException {
    String runJsonFile = this.user + "/" + this.project + "/" + this.appConf
        + "/" + this.appId + "/run.json";
    File rpf = new File(runJsonFile);
    
    Gson gson = new GsonBuilder().create();
    
    if (!rpf.exists()) {
      new File(rpf.getParent()).mkdirs();
    }
    FileWriter wr = new FileWriter(rpf);
    gson.toJson(json, wr);
    wr.flush();
    wr.close();
    
  }
}
