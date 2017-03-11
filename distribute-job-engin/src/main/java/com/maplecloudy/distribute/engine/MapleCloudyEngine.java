package com.maplecloudy.distribute.engine;

import java.lang.reflect.Method;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class MapleCloudyEngine {
  
  public static void main(String[] args) throws Exception {
    // MapleCloudyEngine mce = new MapleCloudyEngine();
    
    final String mainClass = args[0];
    
    // Initialize clients to ResourceManager and NodeManagers
    AMRMClient<ContainerRequest> rmClient = null;
    try {
//      Class<?> klass = Class.forName(mainClass);
      Configuration conf = new YarnConfiguration();
      rmClient = AMRMClient.createAMRMClient();
      rmClient.init(conf);
      rmClient.start();
      
      System.out.println("Classpath         :");
      System.out.println("------------------------");
      StringTokenizer st = new StringTokenizer(
          System.getProperty("java.class.path"), ":");
      System.out.println("Main class        : " + mainClass);
      System.out.println();
      System.out.println();
      System.out.println("Arguments         :");
      for (String arg : args) {
        System.out.println("                    " + arg);
      }
      System.out.println("Java System Properties:");
      System.out.println("------------------------");
      System.getProperties().store(System.out, "");
      System.out.flush();
      
      while (st.hasMoreTokens()) {
        System.out.println("  " + st.nextToken());
      }
      String[] arg = new String[args.length - 1];
      if (args.length > 1) {
        for (int i = 1; i < args.length; i++) {
          arg[i - 1] = args[i];
        }
      }
      
      Class<?> klass = Class.forName(mainClass);
      Method mainMethod = klass.getMethod("main", String[].class);
      mainMethod.invoke(null, (Object) arg);
      
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (rmClient != null) rmClient.unregisterApplicationMaster(
          FinalApplicationStatus.SUCCEEDED, "", "");
    }
    // Un-register with ResourceManager
    
  }
}
