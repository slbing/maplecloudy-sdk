package com.maplecloudy.distribute.engine.app.engineapp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.security.PrivilegedAction;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
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
import com.maplecloudy.distribute.engine.MapleCloudyEngineShellClient;
import com.maplecloudy.distribute.engine.apptask.AppTaskBaseline;
import com.maplecloudy.distribute.engine.nginx.Nginx;
import com.maplecloudy.distribute.engine.nginx.NginxGatewayPara;

public class ClusterEngineAppTask extends AppTaskBaseline {
  
  JSONObject json;
  
  public ClusterEngineAppTask(JSONObject json) {
    super(json);
    this.json = json;
    
  }
  
  @Override
  public void run() {
    try {
      runInfo.clear();
      this.runInfo.add("Start Task!");
      ApplicationId appid = this.checkTaskApp();
      if (appid != null) {
        updateNginx(appid);
        return;
      }
      
      if (!this.checkEnv()) return;
      
      List<String> cmds = Lists.newArrayList();
      cmds.add("-m");
      cmds.add("" + this.memory);
      cmds.add("-cpu");
      cmds.add("" + this.cpu);
      cmds.add("-type");
      cmds.add(this.getAppType());
      cmds.add("-args");
      cmds.add(json.getString("run.shell"));
      
      if (json.has("conf.files")) {
        for (int i = 0; i < json.getJSONArray("conf.files").length(); i++) {
          cmds.add("-f");
          cmds.add(converRemotePath(json.getJSONArray("conf.files")
              .getJSONObject(i).getString("fileName")));
        }
      }
      if (json.has("files")) {
        for (int i = 0; i < json.getJSONArray("files").length(); i++) {
          cmds.add("-f");
          cmds.add(json.getJSONArray("files").getString(0));
        }
      }
      if (json.has("arcs")) {
        for (int i = 0; i < json.getJSONArray("arcs").length(); i++) {
          cmds.add("-arc");
          cmds.add(json.getJSONArray("arcs").getString(i));
        }
      }
      if (json.has("dirs")) {
        for (int i = 0; i < json.getJSONArray("dirs").length(); i++) {
          cmds.add("-dir");
          cmds.add(json.getJSONArray("dirs").getString(i));
        }
      }
      if (this.damon) cmds.add("-damon");
      
      final String[] args = cmds.toArray(new String[cmds.size()]);
      
      final Configuration conf = this.getConf();
      UserGroupInformation ugi = UserGroupInformation.createProxyUser(this.user,
          UserGroupInformation.getLoginUser());
      appid = ugi.doAs(new PrivilegedAction<ApplicationId>() {
        @Override
        public ApplicationId run() {
          try {
            MapleCloudyEngineShellClient mcsc = new MapleCloudyEngineShellClient(
                new YarnConfiguration(conf));
            ApplicationId appid = mcsc.submitApp(args, getName());
            runInfo
                .add("jetty submit yarn sucess,with application id:" + appid);
            return appid;
          } catch (Exception e) {
            e.printStackTrace();
            runInfo.add("run jetty error with:" + e.getMessage());
            return null;
          }
        }
      });
      if (appid != null) {
        this.appids.add(appid);
        
        updateNginx(appid);
      }
      // checkInfo.add("yarn app have submit");
      
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  public void updateNginx(ApplicationId appid)
      throws YarnException, IOException, InterruptedException, JSONException {
    
    if (!this.nginx) return;
    
    boolean updateNginx = true;
    YarnClient yarnClient = YarnClient.createYarnClient();
    
    yarnClient.init(new YarnConfiguration(this.getConf()));
    yarnClient.start();
    while (updateNginx) {
      
      ApplicationReport report = yarnClient.getApplicationReport(appid);
      if (report.getYarnApplicationState() == YarnApplicationState.FAILED
          || report.getYarnApplicationState() == YarnApplicationState.FINISHED
          || report.getYarnApplicationState() == YarnApplicationState.KILLED) {
        runInfo.add("App have run with appid:" + report.getApplicationId()
            + ", and finishedwith with status:"
            + report.getYarnApplicationState());
        updateNginx = false;
      } else if (report
          .getYarnApplicationState() == YarnApplicationState.RUNNING) {
        
        runInfo.add("App have run with appid:" + report.getApplicationId()
            + ", now status" + report.getYarnApplicationState()
            + ",start update nginx!");
        
        NginxGatewayPara ngpara = new NginxGatewayPara();
        
        ngpara.nginxIp = this.nginxIp;
        String host = report.getHost();
        String[] hosts = host.split("/");
        if (hosts.length > 1) host = hosts[1];
        else if (hosts.length > 0) {
          host = hosts[0];
        }
        
        ngpara.appHost = host;
        ngpara.domain = this.nginxDomain;
        ngpara.appPort = this.getPort();
        ngpara.proxyPort = this.proxyPort;
        ngpara.nginxId = this.nginxIp;
        ngpara.appConf = this.appConf;
        ngpara.appType = this.getAppType();
        
        Nginx.updateLocal(ngpara);
        Nginx.updateRemote();
        
        updateNginx = false;
        
        runInfo.add("update nginx finished!");
      } else {
        runInfo.add("App have run with appid:" + report.getApplicationId()
            + ", now status is::" + report.getYarnApplicationState());
      }
      Thread.sleep(5000);
    }
  }
  
  public boolean checkEnv() throws Exception {
    boolean bret = true;
    final Configuration conf = this.getConf();
    FileSystem fs = FileSystem.get(this.getConf());
    // check engint
    bret = checkEngine();
    
    // check files and archives
    String path = "";
    
    if (json.has("files")) {
      for (int i = 0; i < json.getJSONArray("files").length(); i++) {
        
        path = json.getJSONArray("files").getString(i);
        if (fs.exists(new Path(path))) {
          runInfo.add(path + " is ok!");
        } else {
          runInfo.add(path + " not exist!");
          bret = false;
        }
      }
    }
    if (json.has("arcs")) {
      for (int i = 0; i < json.getJSONArray("arcs").length(); i++) {
        
        path = json.getJSONArray("arcs").getString(i);
        if (fs.exists(new Path(path))) {
          runInfo.add(path + " is ok!");
        } else {
          runInfo.add(path + " not exist!");
          bret = false;
        }
      }
    }
    if (json.has("dirs")) {
      for (int i = 0; i < json.getJSONArray("dirs").length(); i++) {
        
        path = json.getJSONArray("dirs").getString(i);
        if (fs.exists(new Path(path))) {
          runInfo.add(path + " is ok!");
        } else {
          runInfo.add(path + " not exist!");
          bret = false;
        }
      }
    }
    // generate confs and upload
    String fileName = "";
    for (int i = 0; i < json.getJSONArray("conf.files").length(); i++) {
      
      JSONObject f = (JSONObject) json.getJSONArray("conf.files").get(i);
      runInfo.add("Generate file with para:" + f.getString("fileName"));
      fileName = processConfigFile(f);
      runInfo.add("GenerateConf sucess: " + fileName);
      boolean b = uploadFile(converRemotePath(fileName));
      if (!b) bret = bret;
    }
    
    // send update ngix
    return bret;
    
  }
  
  public String processConfigFile(JSONObject json)
      throws IOException, JSONException {
    
    String fileName = json.getString("fileName");
    String filePath = this.user + "/" + this.project + "/" + this.appConf + "/"
        + this.appId + "/" + fileName;
    
    File cf = new File(filePath);
    new File(cf.getParent()).mkdirs();
    PrintWriter printWriter = new PrintWriter(filePath);
    BufferedReader bufReader = new BufferedReader(
        new InputStreamReader(new FileInputStream("engine-apps/" + fileName)));
    // new InputStreamReader(this.getClass().getResourceAsStream(fileName)));
    for (String temp = null; (temp = bufReader
        .readLine()) != null; temp = null) {
      
      temp = replacePara(temp, json);
      
      printWriter.append(temp);
      printWriter.append(System.getProperty("line.separator"));// 行与行之间的分割
      
    }
    printWriter.flush();
    printWriter.close();
    
    return fileName;
  }
  
  public boolean uploadFile(final String filePath) throws IOException {
    
    final Configuration conf = this.getConf();
    FileSystem fs = FileSystem.get(this.getConf());
    
    UserGroupInformation ugi = UserGroupInformation.createProxyUser(this.user,
        UserGroupInformation.getLoginUser());
    
    boolean bupload = ugi.doAs(new PrivilegedAction<Boolean>() {
      @Override
      public Boolean run() {
        try {
          FileSystem fs = FileSystem.get(conf);
          fs.copyFromLocalFile(false, true, new Path(filePath),
              new Path(filePath));
        } catch (IllegalArgumentException | IOException e) {
          e.printStackTrace();
          runInfo.add("intall :" + filePath + " error with:" + e.getMessage());
          return false;
        }
        runInfo.add("Install confile succes! " + filePath);
        return true;
      }
      
    });
    return true;
  }
  
  public String replacePara(String str, JSONObject json)
      throws JSONException, IOException {
    
    Iterator it = json.keys();
    String key = "";
    String value = "";
    while (it.hasNext()) {
      
      key = (String) it.next();
      if (key.equals("<port>")) {
        str = str.replace(key, "" + getPort());
      }
      value = json.getString(key);
      str = str.replace(key, value);
    }
    
    return str;
  }
  
  public String converRemotePath(String fileName) {
    return this.user + "/" + this.project + "/" + this.appConf + "/"
        + this.appId + "/" + fileName;
  }
}
