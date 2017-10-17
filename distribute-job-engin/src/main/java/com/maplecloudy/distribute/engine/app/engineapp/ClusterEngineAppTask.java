package com.maplecloudy.distribute.engine.app.engineapp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.maplecloudy.distribute.engine.ClusterEngine;
import com.maplecloudy.distribute.engine.MapleCloudyEngineShellClient;
import com.maplecloudy.distribute.engine.apptask.AppTaskBaseline;
import com.maplecloudy.distribute.engine.nginx.Nginx;
import com.maplecloudy.distribute.engine.nginx.NginxGatewayPara;
import com.maplecloudy.distribute.engine.utils.Config;
import com.maplecloudy.distribute.engine.utils.YarnCompat;
import com.maplecloudy.distribute.engine.utils.YarnUtils;
import com.maplecloudy.yarn.rpc.ClientRpc;

public class ClusterEngineAppTask extends AppTaskBaseline {
  
  public ClusterEngineAppTask(JSONObject json) {
    super(json);
    
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

      UserGroupInformation ugi = UserGroupInformation.createProxyUser(this.user,
          UserGroupInformation.getLoginUser());
      appid = ugi.doAs(new PrivilegedAction<ApplicationId>() {
        @Override
        public ApplicationId run() {
          try {
            ApplicationId appid = runAPP();
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
  
  public ApplicationId runAPP() throws Exception {
 
    
    // Create yarnClient
    // YarnClient yarnClient = ClientRpc.getYarnClient(this.getConf());
    YarnClient yarnClient = YarnClient.createYarnClient();
    
    yarnClient.init(this.getConf());
    yarnClient.start();
    
    // Create application via yarnClient
    
    YarnClientApplication app = yarnClient.createApplication();
    
    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer = Records
        .newRecord(ContainerLaunchContext.class);
    
    List<String> cmds = Lists.newArrayList();
    // don't use -jar since it overrides the classpath
    cmds.add(YarnCompat.$$(ApplicationConstants.Environment.JAVA_HOME)
        + "/bin/java " + ClusterEngine.class.getName() + " 1>"
        + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/"
        + ApplicationConstants.STDOUT + " 2>"
        + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/"
        + ApplicationConstants.STDERR);
    
    amContainer.setCommands(cmds);
    // Setup jar for ApplicationMaster
    
    FileSystem fs = FileSystem.get(this.getConf());
    System.out.println(fs.getCanonicalServiceName());
    HashMap<String,LocalResource> hmlr = Maps.newHashMap();
    
    // add engine
    LocalResource elr = Records.newRecord(LocalResource.class);
    FileStatus enginejar = fs.getFileStatus(new Path(Config.getEngieJar()));
    elr.setResource(ConverterUtils.getYarnUrlFromPath(enginejar.getPath()));
    elr.setSize(enginejar.getLen());
    elr.setTimestamp(enginejar.getModificationTime());
    elr.setType(LocalResourceType.FILE);
    elr.setVisibility(LocalResourceVisibility.PUBLIC);
    hmlr.put(enginejar.getPath().getName(), elr);
    
    amContainer.setLocalResources(hmlr);
    
   
    
    // Setup CLASSPATH for ApplicationMaster
    
    Map<String,String> appMasterEnv = YarnUtils
        .setupAppMasterEnv(this.getConf(), this.json.toString());
    System.out.println("--------------------------");
    System.out.println(appMasterEnv.toString());
    System.out.println("--------------------------");
    amContainer.setEnvironment(appMasterEnv);
    
    // Set up resource type requirements for ApplicationMaster
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(this.json.getInt("ammemory"));
    capability.setVirtualCores(this.json.getInt("amcpu"));
    System.out.println("login user:" + UserGroupInformation.getLoginUser());
    
    // Finally, set-up ApplicationSubmissionContext for the application
    final ApplicationSubmissionContext appContext = app
        .getApplicationSubmissionContext();
    appContext.setApplicationType(type);
//    String name = "engine:launcher:" + type;
    String name = getName();
    appContext.setApplicationName(name); // application name
    appContext.setAMContainerSpec(amContainer);
    appContext.setResource(capability);
    if (this.json.has("queue")) {
    	String queue =(this.json.getString("queue") == null || this.json.getString("queue") == "") ? "default" : this.json.getString("queue");
        appContext.setQueue(queue);
	} else {
        appContext.setQueue("default");	
	}
    appContext.setMaxAppAttempts(1);
    // Submit application
    ApplicationId appId = appContext.getApplicationId();
    System.out.println("Submitting application " + appId);
    System.out.println("--------------------------");
    System.out.println(appContext.toString());
    System.out.println("--------------------------");
    
    yarnClient.submitApplication(appContext);
    yarnClient.close();
    
    return appId;
    
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
  
  private static void usage() {
    String message = "Usage: MapleCloudyEngineClient <cmd> \n" + "\nOptions:\n"
        + "  " + "  -jar  <string>  : jar add to classpath\n"
        + "  -jars     <string>   : dir with all to add classpath\n"
        + "  -p<key=value>   : properties\n"
        + "  -sc<string>   : shell script to can be run\n"
        + "  -args<string>   : args to main class\n"
        + "  -war<string>   : war to start web server\n"
        + "  -arc<string>   : tar package to this cmd\n"
        + "  -m<string>   : memory set for this app,default 256M\n"
        + "  -cpu<string>   : CPU Virtual Cores set for this app, defaule 1\n"
        + "  -damon   : after run the main class, then wait for kill the application\n"
        + "  -type <string>   : the application type ,default is MAPLECLOUDY-APP\n"
        + "  -f <string>   : list of files to be used by this appliation\n"
        + " -dir <string>  : list of dir";
    
    System.err.println(message);
    System.exit(1);
  }
  
}
