package com.maplecloudy.distribute.engine.app.cluster;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.xerces.util.SynchronizedSymbolTable;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.maplecloudy.distribute.engine.utils.StringUtils;
import com.maplecloudy.distribute.engine.utils.YarnCompat;
import com.maplecloudy.yarn.rpc.AppMasterRpc;
import com.maplecloudy.yarn.rpc.NodeMasterRpc;

/**
 * logical cluster managing the global lifecycle for its multiple containers.
 */
public class ClusterContainer implements AutoCloseable {
  
  private static final Log log = LogFactory.getLog(ClusterContainer.class);
  
  private final AppMasterRpc amRpc;
  private final NodeMasterRpc nmRpc;
  private final Configuration cfg;
  private final JSONObject para;
  
  private volatile boolean running = false;
  private volatile boolean clusterHasFailed = false;
  
  private final Set<ContainerId> allocatedContainers = new LinkedHashSet<ContainerId>();
  private final Set<ContainerId> completedContainers = new LinkedHashSet<ContainerId>();
  
  public ClusterContainer(final AppMasterRpc rpc, JSONObject para) {
    this.amRpc = rpc;
    this.cfg = rpc.getConfiguration();
    this.nmRpc = new NodeMasterRpc(cfg, rpc.getNMToCache());
    this.para = para;
    
  }
  
  public void start() throws JSONException {
    running = true;
    nmRpc.start();
    
    UserGroupInformation.setConfiguration(cfg);
    // UserGroupInformation.
    
    UserGroupInformation ugi;
    try {
      ugi = UserGroupInformation.createProxyUser("gxiang",
          UserGroupInformation.getLoginUser());
      ugi.doAs(new PrivilegedAction<ApplicationId>() {
        
        @Override
        public ApplicationId run() {
          try {
            System.out.println("getCurrentUser-----------"
                + UserGroupInformation.getCurrentUser());
            System.out.println("getLoginUser-----------"
                + UserGroupInformation.getLoginUser());
          } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
          }
          
          attemptKeytabLogin();
          
          try {
            log.info(String.format("Allocating  cluster with %d nodes",
                para.getInt("containers")));
            
            // register requests
            Resource capability = YarnCompat.resource(cfg,
                para.getInt("memory"), para.getInt("cpu"));
            Priority prio = Priority.newInstance(-1);
            
            for (int i = 0; i < para.getInt("containers"); i++) {
              // TODO: Add allocation (host/rack rules) - and disable location
              // constraints
              ContainerRequest req = new ContainerRequest(capability, null,
                  null, prio);
              amRpc.addContainerRequest(req);
            }
            
            // update status every 5 sec
            final long heartBeatRate = TimeUnit.SECONDS.toMillis(5);
            
            // start the allocation loop
            // when a new container is allocated, launch it right away
            
            int responseId = 0;
            
            try {
              do {
                AllocateResponse alloc = amRpc.allocate(responseId++);
                List<Container> currentlyAllocated = alloc
                    .getAllocatedContainers();
                for (Container container : currentlyAllocated) {
                  launchContainer(container);
                  allocatedContainers.add(container.getId());
                }
                
                if (currentlyAllocated.size() > 0) {
                  int needed = para.getInt("containers")
                      - allocatedContainers.size();
                  if (needed > 0) {
                    log.info(
                        String.format("%s containers allocated, %s remaining",
                            allocatedContainers.size(), needed));
                  } else {
                    log.info(String.format("Fully allocated %s containers",
                        allocatedContainers.size()));
                  }
                }
                
                List<ContainerStatus> completed = alloc
                    .getCompletedContainersStatuses();
                for (ContainerStatus status : completed) {
                  if (!completedContainers.contains(status.getContainerId())) {
                    ContainerId containerId = status.getContainerId();
                    completedContainers.add(containerId);
                    
                    boolean containerSuccesful = false;
                    
                    switch (status.getExitStatus()) {
                      case ContainerExitStatus.SUCCESS:
                        log.info(String.format(
                            "Container %s finished succesfully...",
                            containerId));
                        containerSuccesful = true;
                        break;
                      case ContainerExitStatus.ABORTED:
                        log.warn(String.format("Container %s aborted...",
                            containerId));
                        break;
                      case ContainerExitStatus.DISKS_FAILED:
                        log.warn(String.format(
                            "Container %s ran out of disk...", containerId));
                        break;
                      case ContainerExitStatus.PREEMPTED:
                        log.warn(String.format("Container %s preempted...",
                            containerId));
                        break;
                      
                      default:
                        log.warn(String.format(
                            "Container %s exited with an invalid/unknown exit code %d",
                            containerId, status.getExitStatus()));
                    }
                    
                    if (!containerSuccesful) {
                      log.warn("Cluster has not completed succesfully...");
                      clusterHasFailed = true;
                      running = false;
                    }
                  }
                }
                
                if (completedContainers.size() == -para.getInt("containers")) {
                  running = false;
                }
                
                if (running) {
                  Thread.sleep(heartBeatRate);
                }
              } while (running);
            } catch (Exception e) {
              e.printStackTrace();
            } finally {
              log.info("Cluster has completed running...");
              try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(15));
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
              try {
                close();
              } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
              }
            }
          } catch (JSONException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
          }
          return null;
        }
      });
      
    } catch (IOException e2) {
      // TODO Auto-generated catch block
      e2.printStackTrace();
    }
    
    // try {
    // System.out.println("getCurrentUser-----------"
    // + UserGroupInformation.getCurrentUser());
    // System.out.println("getLoginUser-----------"
    // + UserGroupInformation.getLoginUser());
    // } catch (IOException e1) {
    // // TODO Auto-generated catch block
    // e1.printStackTrace();
    // }
    // attemptKeytabLogin();
    //
    // log.info(String.format("Allocating cluster with %d nodes",
    // para.getInt("containers")));
    //
    // // register requests
    // Resource capability = YarnCompat.resource(cfg, para.getInt("memory"),
    // para.getInt("cpu"));
    // Priority prio = Priority.newInstance(-1);
    //
    // for (int i = 0; i < para.getInt("containers"); i++) {
    // // TODO: Add allocation (host/rack rules) - and disable location
    // // constraints
    // ContainerRequest req = new ContainerRequest(capability, null, null,
    // prio);
    // amRpc.addContainerRequest(req);
    // }
    //
    // // update status every 5 sec
    // final long heartBeatRate = TimeUnit.SECONDS.toMillis(5);
    //
    // // start the allocation loop
    // // when a new container is allocated, launch it right away
    //
    // int responseId = 0;
    //
    // try {
    // do {
    // AllocateResponse alloc = amRpc.allocate(responseId++);
    // List<Container> currentlyAllocated = alloc.getAllocatedContainers();
    // for (Container container : currentlyAllocated) {
    // launchContainer(container);
    // allocatedContainers.add(container.getId());
    // }
    //
    // if (currentlyAllocated.size() > 0) {
    // int needed = para.getInt("containers") - allocatedContainers.size();
    // if (needed > 0) {
    // log.info(String.format("%s containers allocated, %s remaining",
    // allocatedContainers.size(), needed));
    // } else {
    // log.info(String.format("Fully allocated %s containers",
    // allocatedContainers.size()));
    // }
    // }
    //
    // List<ContainerStatus> completed = alloc
    // .getCompletedContainersStatuses();
    // for (ContainerStatus status : completed) {
    // if (!completedContainers.contains(status.getContainerId())) {
    // ContainerId containerId = status.getContainerId();
    // completedContainers.add(containerId);
    //
    // boolean containerSuccesful = false;
    //
    // switch (status.getExitStatus()) {
    // case ContainerExitStatus.SUCCESS:
    // log.info(String.format("Container %s finished succesfully...",
    // containerId));
    // containerSuccesful = true;
    // break;
    // case ContainerExitStatus.ABORTED:
    // log.warn(String.format("Container %s aborted...", containerId));
    // break;
    // case ContainerExitStatus.DISKS_FAILED:
    // log.warn(String.format("Container %s ran out of disk...",
    // containerId));
    // break;
    // case ContainerExitStatus.PREEMPTED:
    // log.warn(String
    // .format("Container %s preempted...", containerId));
    // break;
    //
    // default:
    // log.warn(String.format(
    // "Container %s exited with an invalid/unknown exit code...",
    // containerId));
    // }
    //
    // if (!containerSuccesful) {
    // log.warn("Cluster has not completed succesfully...");
    // clusterHasFailed = true;
    // running = false;
    // }
    // }
    // }
    //
    // if (completedContainers.size() == -para.getInt("containers")) {
    // running = false;
    // }
    //
    // if (running) {
    // Thread.sleep(heartBeatRate);
    // }
    // } while (running);
    // } catch (Exception e) {
    // e.printStackTrace();
    // } finally {
    // log.info("Cluster has completed running...");
    // try {
    // Thread.sleep(TimeUnit.SECONDS.toMillis(15));
    // } catch (InterruptedException e) {
    // throw new RuntimeException(e);
    // }
    // try {
    // close();
    // } catch (Exception e) {
    // // TODO Auto-generated catch block
    // e.printStackTrace();
    // }
    // }
  }
  
  private void attemptKeytabLogin() {
    // if (UserGroupInformation.isSecurityEnabled()) {
    // try {
    // String localhost = InetAddress.getLocalHost().getCanonicalHostName();
    // String keytabFilename = appConfig.kerberosKeytab();
    // if (keytabFilename == null || keytabFilename.length() == 0) {
    // throw new EsYarnAmException(
    // "Security is enabled, but we could not find a configured keytab; Bailing
    // out...");
    // }
    // String configuredPrincipal = appConfig.kerberosPrincipal();
    // String principal = SecurityUtil.getServerPrincipal(configuredPrincipal,
    // localhost);
    // UserGroupInformation.loginUserFromKeytab(principal, keytabFilename);
    // } catch (UnknownHostException e) {
    // throw new EsYarnAmException(
    // "Could not read localhost information for server principal construction;
    // Bailing out...",
    // e);
    // } catch (IOException e) {
    // throw new EsYarnAmException("Could not log in.", e);
    // }
    // }
  }
  
  private void launchContainer(Container container)
      throws YarnException, IOException, JSONException {
    ContainerLaunchContext ctx = Records
        .newRecord(ContainerLaunchContext.class);
    
    // ctx.setEnvironment(setupEnv(para));
    ctx.setLocalResources(setupResource());
    ctx.setCommands(setupScript());
    
    log.info("About to launch container for command: " + ctx.getCommands());
    
    // setup container
    Map<String,ByteBuffer> startContainer = nmRpc.startContainer(container,
        ctx);
    log.info("Started container " + container);
  }
  
  private Map<String,LocalResource> setupResource()
      throws JSONException, IOException {
    // elasticsearch.zip
    Map<String,LocalResource> resources = new LinkedHashMap<String,LocalResource>();
    
    LocalResource esZip = Records.newRecord(LocalResource.class);
    FileSystem fs = FileSystem.get(cfg);
    String path = "";
    
    System.out.println("resource para:" + para.toString());
    if (para.has("files")) {
      for (int i = 0; i < para.getJSONArray("files").length(); i++) {
        
        path = para.getJSONArray("files").getString(i);
        LocalResource tlr = Records.newRecord(LocalResource.class);
        FileStatus jarf = fs.getFileStatus(new Path(path));
        tlr.setResource(ConverterUtils.getYarnUrlFromPath(jarf.getPath()));
        tlr.setSize(jarf.getLen());
        tlr.setTimestamp(jarf.getModificationTime());
        tlr.setType(LocalResourceType.FILE);
        tlr.setVisibility(LocalResourceVisibility.PUBLIC);
        resources.put(jarf.getPath().getName(), tlr);
        
      }
    }
    if (para.has("arcs")) {
      for (int i = 0; i < para.getJSONArray("arcs").length(); i++) {
        
        path = para.getJSONArray("arcs").getString(i);
        if (fs.exists(new Path(path))) {
          LocalResource tlr = Records.newRecord(LocalResource.class);
          FileStatus jarf = fs.getFileStatus(new Path(path));
          tlr.setResource(ConverterUtils.getYarnUrlFromPath(jarf.getPath()));
          tlr.setSize(jarf.getLen());
          tlr.setTimestamp(jarf.getModificationTime());
          tlr.setType(LocalResourceType.ARCHIVE);
          tlr.setVisibility(LocalResourceVisibility.PUBLIC);
          
          resources.put(jarf.getPath().getName(), tlr);
        }
      }
    }
    if (para.has("dirs")) {
      for (int i = 0; i < para.getJSONArray("dirs").length(); i++) {
        
        path = para.getJSONArray("dirs").getString(i);
        if (fs.exists(new Path(path))) {
          Path pdir = new Path(path);
          if (fs.isDirectory(pdir)) {
            FileStatus[] fss = fs.listStatus(pdir);
            for (FileStatus jfile : fss) {
              LocalResource tlr = Records.newRecord(LocalResource.class);
              
              tlr.setResource(
                  ConverterUtils.getYarnUrlFromPath(jfile.getPath()));
              tlr.setSize(jfile.getLen());
              tlr.setTimestamp(jfile.getModificationTime());
              tlr.setType(LocalResourceType.FILE);
              tlr.setVisibility(LocalResourceVisibility.PUBLIC);
              resources.put(jfile.getPath().getName(), tlr);
            }
          }
        }
      }
      
    }
    // generate confs and upload
    
    for (int i = 0; i < para.getJSONArray("conf.files").length(); i++) {
      
      JSONObject f = (JSONObject) para.getJSONArray("conf.files").get(i);
      path = f.getString("fileName");
      path = converRemotePath(path);
      System.out.println("path:" + path);
      // if (fs.exists(new Path(path))) {
      System.out.println("path exists");
      LocalResource tlr = Records.newRecord(LocalResource.class);
      FileStatus jarf = fs.getFileStatus(new Path(path));
      tlr.setResource(ConverterUtils.getYarnUrlFromPath(jarf.getPath()));
      tlr.setSize(jarf.getLen());
      tlr.setTimestamp(jarf.getModificationTime());
      tlr.setType(LocalResourceType.FILE);
      tlr.setVisibility(LocalResourceVisibility.PUBLIC);
      
      resources.put(jarf.getPath().getName(), tlr);
      // }
    }
    
    System.out.println(
        "resources:" + StringUtils.concatenate(resources.values(), "--"));
    return resources;
  }
  
  private List<String> setupScript() throws JSONException {
    List<String> cmds = new ArrayList<String>();
    // don't use -jar since it overrides the classpath
    // cmds.add("sleep 5000 \n");
    cmds.add("ulimit -a \n");
    cmds.add(para.getString("run.shell"));
    cmds.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/"
        + ApplicationConstants.STDOUT);
    cmds.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/"
        + ApplicationConstants.STDERR);
    System.out.println("run elasticsearch cmd:" + cmds.toString());
    return Collections.singletonList(StringUtils.concatenate(cmds, " "));
  }
  
  public boolean hasFailed() {
    return clusterHasFailed;
  }
  
  public void close() throws Exception {
    running = false;
    nmRpc.close();
  }
  
  public String converRemotePath(String fileName) throws JSONException {
    return para.get("user") + "/" + para.get("project") + "/"
        + para.get("appConf") + "/" + para.get("appId") + "/" + fileName;
  }
}