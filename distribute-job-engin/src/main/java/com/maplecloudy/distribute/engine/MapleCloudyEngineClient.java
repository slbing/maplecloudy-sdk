package com.maplecloudy.distribute.engine;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
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
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class MapleCloudyEngineClient {
  
  Configuration conf = new YarnConfiguration();
  
  private static void usage() {
    String message = "Usage: MapleCloudyEngineClient <mainClass> \n"
        + "\nOptions:\n" + "  " 
        + "  -jar  <string>  : jar add to classpath\n"
        + "  -jars     <string>   : dir with all to add classpath\n"
        + "  -p<key=value>   : properties\n"
        + "  -args<string>   : args to main class\n"
        + "  -war<string>   : war to start web server\n";
    
    System.err.println(message);
    System.exit(1);
  }
  
  public void run(String[] args) throws Exception {
    
    if (args.length < 1) usage();
    String jar = null;
    String jars = null;
    String margs = null;
    String war = null;
    List<String> pps = Lists.newArrayList();
    final String mainClass = args[0];
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
      }
      
    }
    // Create yarnClient
    YarnConfiguration conf = new YarnConfiguration();
    YarnClient yarnClient = YarnClient.createYarnClient();
    
    yarnClient.init(conf);
    yarnClient.start();
    
    // Create application via yarnClient
    YarnClientApplication app = yarnClient.createApplication();
    
    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer = Records
        .newRecord(ContainerLaunchContext.class);
    
    List<String> cmds = Lists.newArrayList();
    String cmd = "$JAVA_HOME/bin/java ";
    for (String pp : pps) {
      cmd += " " + pp;
    }
    cmd += " " + mainClass + " " + margs + " 1>"
        + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + " 2>"
        + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";
    // cmds.add("sleep 600 \n");
    cmds.add(cmd);
    amContainer.setCommands(cmds);
    // Setup jar for ApplicationMaster
    
    FileSystem fs = FileSystem.get(conf);
    System.out.println(fs.getCanonicalServiceName());
    HashMap<String,LocalResource> hmlr = Maps.newHashMap();
    
    // add engine
    LocalResource elr = Records.newRecord(LocalResource.class);
    
    FileStatus enginejar = fs.getFileStatus(new Path(
        "distribute-job-engin-0.3.0-SNAPSHOT.jar"));
    elr.setResource(ConverterUtils.getYarnUrlFromPath(enginejar.getPath()));
    elr.setSize(enginejar.getLen());
    elr.setTimestamp(enginejar.getModificationTime());
    elr.setType(LocalResourceType.FILE);
    elr.setVisibility(LocalResourceVisibility.PUBLIC);
    hmlr.put("distribute-job-engin-0.3.0-SNAPSHOT.jar", elr);
    
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
    
    // add jar
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
          tlr.setVisibility(LocalResourceVisibility.PUBLIC);
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
    capability.setMemory(256);
    capability.setVirtualCores(1);
    
    // Finally, set-up ApplicationSubmissionContext for the application
    ApplicationSubmissionContext appContext = app
        .getApplicationSubmissionContext();
    appContext.setApplicationName("start-tomcat"); // application name
    appContext.setAMContainerSpec(amContainer);
    appContext.setResource(capability);
    appContext.setQueue("default"); // queue
    
    // Submit application
    ApplicationId appId = appContext.getApplicationId();
    System.out.println("Submitting application " + appId);
    System.out.println("--------------------------");
    System.out.println(appContext.toString());
    System.out.println("--------------------------");
    
    yarnClient.submitApplication(appContext);
    
    ApplicationReport appReport = yarnClient.getApplicationReport(appId);
    YarnApplicationState appState = appReport.getYarnApplicationState();
    while (appState != YarnApplicationState.FINISHED
        && appState != YarnApplicationState.KILLED
        && appState != YarnApplicationState.FAILED) {
      Thread.sleep(1000);
      appReport = yarnClient.getApplicationReport(appId);
      appState = appReport.getYarnApplicationState();
    }
    
    System.out.println("Application " + appId + " finished with" + " state "
        + appState + " at " + appReport.getFinishTime());
    
  }
  
  @SuppressWarnings("deprecation")
  private void setupAppMasterEnv(Map<String,String> appMasterEnv) {
    for (String c : conf.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
          c.trim());
    }
    Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
        Environment.PWD.$() + File.separator + "*");
    
  }
  
  public static void main(String[] args) throws Exception {
    MapleCloudyEngineClient c = new MapleCloudyEngineClient();
    c.run(args);
  }
}
