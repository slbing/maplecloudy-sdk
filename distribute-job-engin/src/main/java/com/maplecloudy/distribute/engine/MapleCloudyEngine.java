package com.maplecloudy.distribute.engine;

import java.io.IOException;
import java.lang.reflect.Method;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;

import com.google.common.annotations.VisibleForTesting;

public class MapleCloudyEngine {
  private static final Log LOG = LogFactory.getLog(MapleCloudyEngine.class);
  
  public static void main(String[] args) throws Exception {
    //
    
    final String mainClass = args[0];
    final boolean damon = Boolean.valueOf(args[1]);
    String[] arg = new String[args.length - 2];
    if (args.length > 2) {
      for (int i = 2; i < args.length; i++) {
        arg[i - 2] = args[i];
        
      }
    }
    MapleCloudyEngine mce = new MapleCloudyEngine();
    mce.run(mainClass, arg, damon);
  }
  
  public MapleCloudyEngine() {
    conf = new YarnConfiguration();
  }
  
  // private ByteBuffer allTokens;
  // private UserGroupInformation appSubmitterUgi;
  YarnConfiguration conf;
  // Handle to communicate with the Resource Manager
  @SuppressWarnings("rawtypes")
  private AMRMClientAsync amRMClient;
  
  // Hostname of the container
  private String appMasterHostname = "";
  // Port on which the app master listens for status updates from clients
  private int appMasterRpcPort = -1;
  // Tracking url to which app master publishes info for clients to monitor
  private String appMasterTrackingUrl = "";
  private boolean bRun = true;
  
  public void run(String mainClass, String[] args, boolean damon) {
    
    // Initialize clients to ResourceManager and NodeManagers
    try {
      // Note: Credentials, Token, UserGroupInformation, DataOutputBuffer class
      // are marked as LimitedPrivate
      // Credentials credentials = UserGroupInformation.getCurrentUser()
      // .getCredentials();
      // DataOutputBuffer dob = new DataOutputBuffer();
      // credentials.writeTokenStorageToStream(dob);
      // // Now remove the AM->RM token so that containers cannot access it.
      // Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
      // System.out.println("Executing with tokens:");
      // while (iter.hasNext()) {
      // Token<?> token = iter.next();
      // System.out.println(token);
      // if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
      // iter.remove();
      // }
      // }
      // allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
      
      // Create appSubmitterUgi and add original tokens to it
      // String appSubmitterUserName = System
      // .getenv(ApplicationConstants.Environment.USER.name());
      // appSubmitterUgi = UserGroupInformation
      // .createRemoteUser(appSubmitterUserName);
      // appSubmitterUgi.addCredentials(credentials);
      // WebApp wa = WebApps.$for(myApp).at(address, port).
      // * with(configuration).
      // * start(new WebApp() {
      // * &#064;Override public void setup() {
      // * route("/foo/action", FooController.class);
      // * route("/foo/:id", FooController.class, "show");
      // * }
      // * });</pre>
      
      AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
      amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
      amRMClient.init(conf);
      amRMClient.start();
      
      appMasterHostname = NetUtils.getHostname();
      RegisterApplicationMasterResponse response = amRMClient
          .registerApplicationMaster(appMasterHostname, appMasterRpcPort,
              appMasterTrackingUrl);
      
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
      
      Class<?> klass = Class.forName(mainClass);
      Method mainMethod = klass.getMethod("main", String[].class);
      mainMethod.invoke(null, (Object) args);
      System.out.println("Mainclass invoke sucesss-------------------!:");
      
      if (damon) {
        while (bRun) {
          Thread.sleep(5000);
          System.out.println("Heart beat at damon model");
        }
      }
      amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
          "run main class success", appMasterTrackingUrl);
    } catch (Exception e) {
      e.printStackTrace();
      try {
        amRMClient.unregisterApplicationMaster(FinalApplicationStatus.FAILED,
            e.getMessage(), appMasterTrackingUrl);
      } catch (YarnException e1) {
        e1.printStackTrace();
      } catch (IOException e1) {
        e1.printStackTrace();
      }
      
    } finally {
      Runtime.getRuntime().exit(0);
      amRMClient.stop();
    }
  }
  
  @VisibleForTesting
  class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
    
    public void onContainersCompleted(
        List<ContainerStatus> completedContainers) {
      
    }
    
    @Override
    public void onContainersAllocated(List<Container> allocatedContainers) {}
    
    @Override
    public void onShutdownRequest() {
      bRun = false;
    }
    
    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {}
    
    @Override
    public void onError(Throwable e) {
      LOG.error("Error in RMCallbackHandler: ", e);
      bRun = false;
    }
    
    @Override
    public float getProgress() {
      return 0;
    }
  }
  
  // static class NMCallbackHandler implements NMClientAsync.CallbackHandler {
  //
  // private final MapleCloudyEngine applicationMaster;
  //
  // public NMCallbackHandler(MapleCloudyEngine applicationMaster) {
  // this.applicationMaster = applicationMaster;
  // }
  //
  // @Override
  // public void onContainerStarted(ContainerId containerId,
  // Map<String,ByteBuffer> allServiceResponse) {
  //
  // }
  //
  // @Override
  // public void onContainerStatusReceived(ContainerId containerId,
  // ContainerStatus containerStatus) {
  // // TODO Auto-generated method stub
  //
  // }
  //
  // @Override
  // public void onContainerStopped(ContainerId containerId) {
  // // TODO Auto-generated method stub
  //
  // }
  //
  // @Override
  // public void onStartContainerError(ContainerId containerId, Throwable t) {
  // // TODO Auto-generated method stub
  //
  // }
  //
  // @Override
  // public void onGetContainerStatusError(ContainerId containerId, Throwable t)
  // {
  // // TODO Auto-generated method stub
  //
  // }
  //
  // @Override
  // public void onStopContainerError(ContainerId containerId, Throwable t) {
  // // TODO Auto-generated method stub
  //
  // }
  //
  // }
  //
}
