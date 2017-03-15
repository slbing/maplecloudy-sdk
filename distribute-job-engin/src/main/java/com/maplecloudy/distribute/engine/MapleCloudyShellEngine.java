package com.maplecloudy.distribute.engine;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.google.common.annotations.VisibleForTesting;

public class MapleCloudyShellEngine {
  private static final Log LOG = LogFactory
      .getLog(MapleCloudyShellEngine.class);
  
  public static void main(String[] args) throws Exception {
    //
    System.out.println(StringUtils.join(args,"\n"));
    
    final String cmd = args[0];
    boolean damon = false;
    if (args.length > 1) damon = Boolean.valueOf(args[1]);
    
    MapleCloudyShellEngine mce = new MapleCloudyShellEngine();
    mce.run(cmd, damon);
    
  }
  
  public MapleCloudyShellEngine() {
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
  
  public void run(String cmd, boolean damon) {
    
    // Initialize clients to ResourceManager and NodeManagers
    try {
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
      System.out.println("Cmd        : " + cmd);
      System.out.println();
      System.out.println();
      System.out.println("Java System Properties:");
      System.out.println("------------------------");
      System.getProperties().store(System.out, "");
      System.out.flush();
      
      while (st.hasMoreTokens()) {
        System.out.println("  " + st.nextToken());
      }
      String[] cmds = cmd.split(":");
      for (String subcmd : cmds) {
        runCmd(subcmd);
      }
      System.out.println("Cmd run sucesss-------------------!:");
      
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
    
    public void onContainersCompleted(List<ContainerStatus> completedContainers) {
      
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
  
  public void runCmd(String cmd) {
    try {
      Map<String,String> env = System.getenv();
      ArrayList<String> envList = new ArrayList<String>();
      
      for (Map.Entry<String,String> entry : env.entrySet()) {
        String key = entry.getKey();
        String value = entry.getValue();
        envList.add(key + "=" + value);
      }
      String[] envAM = new String[envList.size()];
      
      Process amProc = Runtime.getRuntime().exec(cmd, envList.toArray(envAM));
      
      final BufferedReader errReader = new BufferedReader(
          new InputStreamReader(amProc.getErrorStream(),
              Charset.forName("UTF-8")));
      final BufferedReader inReader = new BufferedReader(new InputStreamReader(
          amProc.getInputStream(), Charset.forName("UTF-8")));
      
      // read error and input streams as this would free up the buffers
      // free the error stream buffer
      Thread errThread = new Thread() {
        @Override
        public void run() {
          try {
            String line = errReader.readLine();
            while ((line != null) && !isInterrupted()) {
              System.err.println("cmd output------:" + line);
              line = errReader.readLine();
            }
          } catch (IOException ioe) {
            LOG.warn("Error reading the error stream", ioe);
          }
        }
      };
      Thread outThread = new Thread() {
        @Override
        public void run() {
          try {
            String line = inReader.readLine();
            while ((line != null) && !isInterrupted()) {
              System.out.println("cmd output------:" + line);
              line = inReader.readLine();
            }
          } catch (IOException ioe) {
            LOG.warn("Error reading the out stream", ioe);
          }
        }
      };
      try {
        errThread.start();
        outThread.start();
      } catch (IllegalStateException ise) {}
      
      // wait for the process to finish and check the exit code
      try {
        int exitCode = amProc.waitFor();
        LOG.info("AM process exited with value: " + exitCode);
      } catch (InterruptedException e) {
        e.printStackTrace();
      } finally {}
      
      try {
        // make sure that the error thread exits
        // on Windows these threads sometimes get stuck and hang the execution
        // timeout and join later after destroying the process.
        errThread.join();
        outThread.join();
        errReader.close();
        inReader.close();
      } catch (InterruptedException ie) {
        LOG.info(
            "ShellExecutor: Interrupted while reading the error/out stream", ie);
      } catch (IOException ioe) {
        LOG.warn("Error while closing the error/out stream", ioe);
      }
      amProc.destroy();
    } catch (Exception e) {
      e.printStackTrace();
      LOG.warn("Shell run with exception", e);
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
