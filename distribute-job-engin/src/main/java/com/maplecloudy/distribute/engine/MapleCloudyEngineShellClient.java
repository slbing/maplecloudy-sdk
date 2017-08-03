package com.maplecloudy.distribute.engine;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.maplecloudy.distribute.engine.utils.Config;

public class MapleCloudyEngineShellClient extends Configured implements Tool {
  
  // Configuration conf = new YarnConfiguration();
  public MapleCloudyEngineShellClient(Configuration conf) {
    this.setConf(conf);
  }
  
  private static void usage() {
    String message = "Usage: MapleCloudyEngineClient <cmd> \n"
        + "\nOptions:\n"
        + "  "
        + "  -jar  <string>  : jar add to classpath\n"
        + "  -jars     <string>   : dir with all to add classpath\n"
        + "  -p<key=value>   : properties\n"
        + "  -sc<string>   : shell script to can be run\n"
        + "  -args<string>   : args to main class\n"
        + "  -war<string>   : war to start web server\n"
        + "  -arc<string>   : tar package to this cmd\n"
        + "  -m<string>   : memory set for this app,default 256M\n"
        + "  -cpu<string>   : CPU Virtual Cores set for this app, defaule 1\n"
        + "  -damon   : after run the main class, then wait for kill the application\n"
        + "  -type <string>   : the application type ,default is MAPLECLOUDY-APP\n";
    
    System.err.println(message);
    System.exit(1);
  }
  
  public ApplicationId submitApp(String[] args, String name) throws Exception {
    if (args.length < 1) usage();
    String jar = null;
    String jars = null;
    String margs = null;
    String war = null;
    List<String> arcs = Lists.newArrayList();
    int memory = 256;
    boolean damon = false;
    String shellScript = null;
    String type = "MAPLECLOUDY-APP";
    int cpu = 1;
    List<String> pps = Lists.newArrayList();
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-jar")) {
        i++;
        if (i >= args.length) {
          usage();
        }
        jar = args[i];
      } else if (args[i].equals("-jars")) {
        i++;
        if (i >= args.length) {
          usage();
        }
        jars = args[i];
      } else if (args[i].equals("-sc")) {
        i++;
        if (i >= args.length) {
          usage();
        }
        shellScript = args[i];
      } else if (args[i].equals("-p")) {
        pps.add(args[i].substring(2));
      } else if (args[i].equals("-war")) {
        i++;
        if (i >= args.length) {
          usage();
        }
        war = args[i];
      } else if (args[i].equals("-args")) {
        i++;
        if (i >= args.length) {
          usage();
        }
        margs = args[i];
      } else if (args[i].equals("-m")) {
        i++;
        if (i >= args.length) {
          usage();
        }
        memory = Integer.parseInt(args[i]);
      } else if (args[i].equals("-cpu")) {
        i++;
        if (i >= args.length) {
          usage();
        }
        cpu = Integer.parseInt(args[i]);
      } else if (args[i].equals("-damon")) {
        damon = true;
        i++;
        
      } else if (args[i].equals("-type")) {
        i++;
        if (i >= args.length) {
          usage();
        }
        type = args[i];
      } else if (args[i].equals("-arc")) {
        i++;
        if (i >= args.length) {
          usage();
        }
        arcs.add(args[i]);
      }
    }
    // Create yarnClient
    
    // conf.set("fs.defaultFS",
    // "hdfs://zztz01.cluster-test.maplecloudy.com:8020");
    // conf.set("yarn.resourcemanager.address",
    // "zztz01.cluster-test.maplecloudy.com:8032");
    // conf.set("yarn.resourcemanager.admin.address",
    // "zztz01.cluster-test.maplecloudy.com:8033");
    // conf.set("yarn.resourcemanager.scheduler.address",
    // "zztz01.cluster-test.maplecloudy.com:8030");
    // conf.set("yarn.resourcemanager.resource-tracker.address",
    // "zztz01.cluster-test.maplecloudy.com:8031");
    // conf.set("yarn.resourcemanager.webapp.address",
    // "zztz01.cluster-test.maplecloudy.com:8088");
    // conf.set("yarn.resourcemanager.webapp.https.address",
    // "zztz01.cluster-test.maplecloudy.com:8090");
    // conf.set("yarn.application.classpath",
    // "$HADOOP_CLIENT_CONF_DIR,$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*");
    
    YarnClient yarnClient = YarnClient.createYarnClient();
    
    yarnClient.init(this.getConf());
    yarnClient.start();
    
    // Create application via yarnClient
    
    YarnClientApplication app = yarnClient.createApplication();
    
    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer = Records
        .newRecord(ContainerLaunchContext.class);
    
    List<String> cmds = Lists.newArrayList();
    // cmds.add("sleep 600 \n");
    String cmd = "$JAVA_HOME/bin/java ";
    for (String pp : pps) {
      cmd += " " + pp;
    }
    cmd += " com.maplecloudy.distribute.engine.MapleCloudyShellEngine   '"
        + margs + "' " + damon + " 1>"
        + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + " 2>"
        + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";
    
    System.out.println("command:" + cmd.toString());
    ;
    cmds.add(cmd);
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
    
    // add jar
    if (jar != null) {
      LocalResource tlr = Records.newRecord(LocalResource.class);
      FileStatus jarf = fs.getFileStatus(new Path(jar));
      tlr.setResource(ConverterUtils.getYarnUrlFromPath(jarf.getPath()));
      tlr.setSize(jarf.getLen());
      tlr.setTimestamp(jarf.getModificationTime());
      tlr.setType(LocalResourceType.FILE);
      tlr.setVisibility(LocalResourceVisibility.PUBLIC);
      hmlr.put(jarf.getPath().getName(), tlr);
    }
    
    // add shellScript
    if (shellScript != null) {
      LocalResource tlr = Records.newRecord(LocalResource.class);
      FileStatus jarf = fs.getFileStatus(new Path(shellScript));
      tlr.setResource(ConverterUtils.getYarnUrlFromPath(jarf.getPath()));
      tlr.setSize(jarf.getLen());
      tlr.setTimestamp(jarf.getModificationTime());
      tlr.setType(LocalResourceType.FILE);
      tlr.setVisibility(LocalResourceVisibility.PUBLIC);
      hmlr.put(jarf.getPath().getName(), tlr);
    }
    
    // add war
    if (war != null) {
      LocalResource tlr = Records.newRecord(LocalResource.class);
      FileStatus jarf = fs.getFileStatus(new Path(war));
      tlr.setResource(ConverterUtils.getYarnUrlFromPath(jarf.getPath()));
      tlr.setSize(jarf.getLen());
      tlr.setTimestamp(jarf.getModificationTime());
      tlr.setType(LocalResourceType.FILE);
      tlr.setVisibility(LocalResourceVisibility.PUBLIC);
      hmlr.put(jarf.getPath().getName(), tlr);
    }
    
    for (String arc : arcs) {
      LocalResource tlr = Records.newRecord(LocalResource.class);
      FileStatus jarf = fs.getFileStatus(new Path(arc));
      tlr.setResource(ConverterUtils.getYarnUrlFromPath(jarf.getPath()));
      tlr.setSize(jarf.getLen());
      tlr.setTimestamp(jarf.getModificationTime());
      tlr.setType(LocalResourceType.ARCHIVE);
      tlr.setVisibility(LocalResourceVisibility.PUBLIC);
      
      hmlr.put(jarf.getPath().getName(), tlr);
    }
    if (jars != null) {
      Path pjars = new Path(jar);
      if (fs.isDirectory(pjars)) {
        FileStatus[] fss = fs.listStatus(pjars);
        for (FileStatus jfile : fss) {
          LocalResource tlr = Records.newRecord(LocalResource.class);
          
          tlr.setResource(ConverterUtils.getYarnUrlFromPath(jfile.getPath()));
          tlr.setSize(jfile.getLen());
          tlr.setTimestamp(jfile.getModificationTime());
          tlr.setType(LocalResourceType.FILE);
          tlr.setVisibility(LocalResourceVisibility.PRIVATE);
          hmlr.put(jfile.getPath().getName(), tlr);
        }
      }
    }
    // add war
    // LocalResource wlr = Records.newRecord(LocalResource.class);
    // FileStatus wfs = fs.getFileStatus(warpath);
    // wlr.setResource(ConverterUtils.getYarnUrlFromPath(wfs.getPath()));
    // wlr.setSize(wfs.getLen());
    // wlr.setTimestamp(wfs.getModificationTime());
    // wlr.setType(LocalResourceType.FILE);
    // wlr.setVisibility(LocalResourceVisibility.PUBLIC);
    // hmlr.put("tomcat/apache-tomcat-8.5.9/webapps/", wlr);
    
    amContainer.setLocalResources(hmlr);
    
    // Setup CLASSPATH for ApplicationMaster
    Map<String,String> appMasterEnv = new HashMap<String,String>();
    setupAppMasterEnv(appMasterEnv);
    System.out.println("--------------------------");
    System.out.println(appMasterEnv.toString());
    System.out.println("--------------------------");
    amContainer.setEnvironment(appMasterEnv);
    
    // Set up resource type requirements for ApplicationMaster
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(memory);
    capability.setVirtualCores(cpu);
    
    // Finally, set-up ApplicationSubmissionContext for the application
    ApplicationSubmissionContext appContext = app
        .getApplicationSubmissionContext();
    appContext.setApplicationType(type);
    if (name == null) {
      name = "engine:launcher:";
      if (jar != null) name += ":" + jar;
      if (jars != null) name += ":" + jars;
      if (war != null) name += ":" + war;
      if (margs != null) margs += ":" + margs;
    }
    appContext.setApplicationName(name); // application name
    appContext.setAMContainerSpec(amContainer);
    appContext.setResource(capability);
    appContext.setQueue("default"); // queue
    appContext.setMaxAppAttempts(1);
    
    // Submit application
    ApplicationId appId = appContext.getApplicationId();
    System.out.println("Submitting application " + appId);
    System.out.println("--------------------------");
    System.out.println(appContext.toString());
    System.out.println("--------------------------");
    
    ApplicationId appid = yarnClient.submitApplication(appContext);
    yarnClient.close();
    return appid;
  }
  
  public int run(String[] args) {
    
    try {
      this.submitApp(args, null);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    return 0;
    // ApplicationReport appReport = yarnClient.getApplicationReport(appId);
    // YarnApplicationState appState = appReport.getYarnApplicationState();
    // while (appState != YarnApplicationState.FINISHED
    // && appState != YarnApplicationState.KILLED
    // && appState != YarnApplicationState.FAILED) {
    // Thread.sleep(1000);
    // appReport = yarnClient.getApplicationReport(appId);
    // appState = appReport.getYarnApplicationState();
    // }
    //
    // System.out.println("Application " + appId + " finished with" + " state "
    // + appState + " at " + appReport.getFinishTime());
    
  }
  
  private void setupAppMasterEnv(Map<String,String> appMasterEnv) {
    
    StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$())
        .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
    for (String c : this.getConf().getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
      classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
      classPathEnv.append(c.trim());
    }
    appMasterEnv.put(Environment.CLASSPATH.name(), classPathEnv.toString());
    
  }
  
  public static void main(final String[] args) throws Exception {
    String user = "maplecloudy";
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-user")) {
        i++;
        if (i >= args.length) {
          usage();
        }
        user = args[i];
      }
    }
    System.out.println("login in user:" + UserGroupInformation.getLoginUser());
    UserGroupInformation ugi = UserGroupInformation.createProxyUser(user,
        UserGroupInformation.getLoginUser());
    ugi.doAs(new PrivilegedAction<Void>() {
      @Override
      public Void run() {
        try {
          ToolRunner.run(new MapleCloudyEngineShellClient(
              new YarnConfiguration()), args);
        } catch (Exception e) {
          e.printStackTrace();
        }
        return null;
      }
      
    });
    // System.exit(0);
  }
}
