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

public class StartTomcatClient {

	Configuration conf = new YarnConfiguration();
	
	  private static void usage()
	    {
	        String message = "Usage: StartTomcatClient <tomcatpath> <warpath>\n";
	        
	        System.err.println(message);
	        System.exit( 1 );
	    }
	public void run(String[] args) throws Exception {
		
		if(args.length < 2)
			usage();
		
		final Path tomcatPath = new Path(args[0]);
		final Path warpath = new Path(args[1]);
		

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
//		cmds.add("sleep 600 \n");
		cmds.add("$JAVA_HOME/bin/java"
		    +" -Dcatalina.base=tomcat/apache-tomcat-8.5.9"
        +" -Dcatalina.home=tomcat/apache-tomcat-8.5.9"
        +" com.maplecloudy.distribute.engine.MapleCloudyEngine"
        +" tomcat"
        + " 1>"
        + ApplicationConstants.LOG_DIR_EXPANSION_VAR
        + "/stdout" + " 2>"
        + ApplicationConstants.LOG_DIR_EXPANSION_VAR
        + "/stderr");
		amContainer
		.setCommands(cmds);
		// Setup jar for ApplicationMaster

		FileSystem fs = FileSystem.get(conf);
		System.out.println(fs.getCanonicalServiceName());
		HashMap<String, LocalResource> hmlr = Maps.newHashMap();
		
		//add engine
		LocalResource elr = Records.newRecord(LocalResource.class);
	
    FileStatus enginejar = fs.getFileStatus(new Path("distribute-job-engin-0.3.0-SNAPSHOT.jar"));
    elr.setResource(ConverterUtils.getYarnUrlFromPath(enginejar.getPath()));
    elr.setSize(enginejar.getLen());
    elr.setTimestamp(enginejar.getModificationTime());
    elr.setType(LocalResourceType.FILE);
    elr.setVisibility(LocalResourceVisibility.PUBLIC);
    hmlr.put("distribute-job-engin-0.3.0-SNAPSHOT.jar", elr);
    
		//add tomcat 
		LocalResource tlr = Records.newRecord(LocalResource.class);
		FileStatus tomcatfile = fs.getFileStatus(tomcatPath);
		tlr.setResource(ConverterUtils.getYarnUrlFromPath(tomcatfile.getPath()));
		tlr.setSize(tomcatfile.getLen());
		tlr.setTimestamp(tomcatfile.getModificationTime());
		tlr.setType(LocalResourceType.ARCHIVE);
		tlr.setVisibility(LocalResourceVisibility.PUBLIC);
		hmlr.put("tomcat", tlr);
		//add war
		LocalResource wlr = Records.newRecord(LocalResource.class);
		FileStatus wfs = fs.getFileStatus(warpath);
		wlr.setResource(ConverterUtils.getYarnUrlFromPath(wfs.getPath()));
		wlr.setSize(wfs.getLen());
		wlr.setTimestamp(wfs.getModificationTime());
		wlr.setType(LocalResourceType.FILE);
		wlr.setVisibility(LocalResourceVisibility.PUBLIC);
		hmlr.put("tomcat/apache-tomcat-8.5.9/webapps/", wlr);
		amContainer.setLocalResources(hmlr);
		

		// Setup CLASSPATH for ApplicationMaster
		Map<String, String> appMasterEnv = new HashMap<String, String>();
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
		
		System.out.println("Application " + appId + " finished with"
				+ " state " + appState + " at " + appReport.getFinishTime());

	}

	@SuppressWarnings("deprecation")
	private void setupAppMasterEnv(Map<String, String> appMasterEnv) {
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
		StartTomcatClient c = new StartTomcatClient();
		c.run(args);
	}
}
