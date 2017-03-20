package com.maplecloudy.distribute.engine;

import java.io.File;
import java.io.FilenameFilter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class MapleCloudyEngineTomcat {
  
  public static void main(String[] args) throws Exception {
    System.out.println(System.getProperty("catalina.base"));
    System.out.println(System.getProperty("catalina.home"));
    MapleCloudyEngineTomcat mce = new MapleCloudyEngineTomcat();
    
    final String command = args[0];
    // Initialize clients to ResourceManager and NodeManagers
    Configuration conf = new YarnConfiguration();
    AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
    try {
      rmClient.init(conf);
      rmClient.start();
      mce.runCmd(command);
    } catch (Exception e) {
      e.printStackTrace();
    }
    // Un-register with ResourceManager
    rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "",
        "");
  }
  
  private static Method addURL = initAddMethod();
  
  private static URLClassLoader classloader = (URLClassLoader) ClassLoader
      .getSystemClassLoader();
  
  private static Method initAddMethod() {
    Method add = null;
    try {
      add = URLClassLoader.class.getDeclaredMethod("addURL",
          new Class[] {URL.class});
      add.setAccessible(true);
      
    } catch (NoSuchMethodException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (SecurityException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return add;
  }
  
  private static void addURL(File file) {
    try {
      addURL.invoke(classloader, new Object[] {file.toURI().toURL()});
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  public void runCmd(String command) throws ClassNotFoundException,
      NoSuchMethodException, SecurityException, IllegalAccessException,
      IllegalArgumentException, InvocationTargetException {
    
    // addURL(new File("tomcat/apache-tomcat-8.5.9/bin/bootstrap.jar"));
    File libs = new File("tomcat/apache-tomcat-8.5.9/bin/");
    for (File lib : libs.listFiles(new FilenameFilter() {
      
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith("jar");
      }
      
    })) {
      addURL(lib);
    }
    libs = new File("tomcat/apache-tomcat-8.5.9/lib/");
    for (File lib : libs.listFiles(new FilenameFilter() {
      
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith("jar");
      }
      
    })) {
      addURL(lib);
    }
    
    Class<?> klass = Class.forName("org.apache.catalina.startup.Bootstrap",
        true, classloader);
    System.out.println("Classpath         :");
    System.out.println("------------------------");
    StringTokenizer st = new StringTokenizer(System.getProperty("java.class.path"), ":");
    while (st.hasMoreTokens()) {
        System.out.println("  " + st.nextToken());
    }
    // System.out.println(System.getProperty("catalina.base"));
    // System.out.println(System.getProperty("catalina.home"));
    // System.setProperty("catalina.base", ".");
    // System.setProperty("catalina.home", "tomcat/apache-tomcat-8.5.9");
    // System.out.println(System.getProperty("catalina.base"));
    // System.out.println(System.getProperty("catalina.home"));
    String[] args = new String[1];
    args[0] = "start";
    
    
    Method mainMethod = klass.getMethod("main", String[].class);
    mainMethod.invoke(null, (Object) args);
  }
}
