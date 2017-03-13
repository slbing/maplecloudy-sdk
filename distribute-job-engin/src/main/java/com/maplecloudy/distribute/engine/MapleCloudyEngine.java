package com.maplecloudy.distribute.engine;

import java.lang.reflect.Method;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.google.common.annotations.VisibleForTesting;

public class MapleCloudyEngine {
  private static final Log LOG = LogFactory.getLog(MapleCloudyEngine.class);
  
  public static void main(String[] args) throws Exception {
    //
    
    final String mainClass = args[0];
    String[] arg = new String[args.length - 1];
    if (args.length > 1) {
      for (int i = 1; i < args.length; i++) {
        arg[i - 1] = args[i];
      }
    }
    
    MapleCloudyEngine mce = new MapleCloudyEngine();
    mce.run(mainClass, arg);
    
  }
  
  public MapleCloudyEngine() {
    conf = new YarnConfiguration();
  }
  
  // private ByteBuffer allTokens;
  //  private UserGroupInformation appSubmitterUgi;
  YarnConfiguration conf;
  // Handle to communicate with the Resource Manager
  @SuppressWarnings("rawtypes")
  private AMRMClientAsync amRMClient;
  // Handle to communicate with the Node Manager
  private NMClientAsync nmClientAsync;
  
  // Hostname of the container
  private String appMasterHostname = "";
  // Port on which the app master listens for status updates from clients
  private int appMasterRpcPort = -1;
  // Tracking url to which app master publishes info for clients to monitor
  private String appMasterTrackingUrl = "";
  
  public void run(String mainClass, String[] args) {
    
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
      
    } catch (Exception e) {
      e.printStackTrace();
    } finally {

    }
  }
  
  @VisibleForTesting
  class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
    
    public void onContainersCompleted(List<ContainerStatus> completedContainers) {
      
    }
    
    @Override
    public void onContainersAllocated(List<Container> allocatedContainers) {}
    
    @Override
    public void onShutdownRequest() {
      
    }
    
    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {}
    
    @Override
    public void onError(Throwable e) {
      LOG.error("Error in RMCallbackHandler: ", e);
      amRMClient.stop();
    }
    
    @Override
    public float getProgress() {
      return 0;
    }
  }
  
//  static class NMCallbackHandler implements NMClientAsync.CallbackHandler {
//    
//    private final MapleCloudyEngine applicationMaster;
//    
//    public NMCallbackHandler(MapleCloudyEngine applicationMaster) {
//      this.applicationMaster = applicationMaster;
//    }
//    
//    @Override
//    public void onContainerStarted(ContainerId containerId,
//        Map<String,ByteBuffer> allServiceResponse) {
//      
//    }
//    
//    @Override
//    public void onContainerStatusReceived(ContainerId containerId,
//        ContainerStatus containerStatus) {
//      // TODO Auto-generated method stub
//      
//    }
//    
//    @Override
//    public void onContainerStopped(ContainerId containerId) {
//      // TODO Auto-generated method stub
//      
//    }
//    
//    @Override
//    public void onStartContainerError(ContainerId containerId, Throwable t) {
//      // TODO Auto-generated method stub
//      
//    }
//    
//    @Override
//    public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
//      // TODO Auto-generated method stub
//      
//    }
//    
//    @Override
//    public void onStopContainerError(ContainerId containerId, Throwable t) {
//      // TODO Auto-generated method stub
//      
//    }
//    
//  }
//  
}
