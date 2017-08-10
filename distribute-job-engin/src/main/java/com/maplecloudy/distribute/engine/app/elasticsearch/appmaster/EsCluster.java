package com.maplecloudy.distribute.engine.app.elasticsearch.appmaster;
import java.io.IOException;
import java.nio.ByteBuffer;
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

import com.maplecloudy.distribute.engine.app.elasticsearch.ElasticsearchInstallInfo;
import com.maplecloudy.distribute.engine.app.elasticsearch.ElatisticSearchPara;
import com.maplecloudy.distribute.engine.utils.Config;
import com.maplecloudy.distribute.engine.utils.StringUtils;
import com.maplecloudy.distribute.engine.utils.YarnCompat;
import com.maplecloudy.distribute.engine.utils.YarnUtils;


/**
 * logical cluster managing the global lifecycle for its multiple containers.
 */
class EsCluster implements AutoCloseable {
  
  private static final Log log = LogFactory.getLog(EsCluster.class);
  
  private final AppMasterRpc amRpc;
  private final NodeMasterRpc nmRpc;
  private final Configuration cfg;
  private final ElatisticSearchPara para;
  private final Map<String,String> masterEnv;
  
  private volatile boolean running = false;
  private volatile boolean clusterHasFailed = false;
  
  private final Set<ContainerId> allocatedContainers = new LinkedHashSet<ContainerId>();
  private final Set<ContainerId> completedContainers = new LinkedHashSet<ContainerId>();
  
  public EsCluster(final AppMasterRpc rpc, ElatisticSearchPara para,
      Map<String,String> masterEnv) {
    this.amRpc = rpc;
    this.cfg = rpc.getConfiguration();
    this.nmRpc = new NodeMasterRpc(cfg, rpc.getNMToCache());
    this.para = para;
    this.masterEnv = masterEnv;
  }
  
  public void start() {
    running = true;
    nmRpc.start();
    
    UserGroupInformation.setConfiguration(cfg);
    // UserGroupInformation.
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
    
    log.info(String.format("Allocating Elasticsearch cluster with %d nodes",
        para.containers));
    
    // register requests
    Resource capability = YarnCompat.resource(cfg, para.memory,
        para.cpu);
    Priority prio = Priority.newInstance(-1);
    
    for (int i = 0; i < para.containers; i++) {
      // TODO: Add allocation (host/rack rules) - and disable location
      // constraints
      ContainerRequest req = new ContainerRequest(capability, null, null, prio);
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
        List<Container> currentlyAllocated = alloc.getAllocatedContainers();
        for (Container container : currentlyAllocated) {
          launchContainer(container);
          allocatedContainers.add(container.getId());
        }
        
        if (currentlyAllocated.size() > 0) {
          int needed = para.containers
              - allocatedContainers.size();
          if (needed > 0) {
            log.info(String.format("%s containers allocated, %s remaining",
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
                log.info(String.format("Container %s finished succesfully...",
                    containerId));
                containerSuccesful = true;
                break;
              case ContainerExitStatus.ABORTED:
                log.warn(String.format("Container %s aborted...", containerId));
                break;
              case ContainerExitStatus.DISKS_FAILED:
                log.warn(String.format("Container %s ran out of disk...",
                    containerId));
                break;
              case ContainerExitStatus.PREEMPTED:
                log.warn(String
                    .format("Container %s preempted...", containerId));
                break;
              
              default:
                log.warn(String.format(
                    "Container %s exited with an invalid/unknown exit code...",
                    containerId));
            }
            
            if (!containerSuccesful) {
              log.warn("Cluster has not completed succesfully...");
              clusterHasFailed = true;
              running = false;
            }
          }
        }
        
        if (completedContainers.size() == para.containers) {
          running = false;
        }
        
        if (running) {
            Thread.sleep(heartBeatRate);
        }
      } while (running);
    }catch(Exception e)
    {
      e.printStackTrace();
    }
    finally {
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
  }
  
  private void attemptKeytabLogin() {
//    if (UserGroupInformation.isSecurityEnabled()) {
//      try {
//        String localhost = InetAddress.getLocalHost().getCanonicalHostName();
//        String keytabFilename = appConfig.kerberosKeytab();
//        if (keytabFilename == null || keytabFilename.length() == 0) {
//          throw new EsYarnAmException(
//              "Security is enabled, but we could not find a configured keytab; Bailing out...");
//        }
//        String configuredPrincipal = appConfig.kerberosPrincipal();
//        String principal = SecurityUtil.getServerPrincipal(configuredPrincipal,
//            localhost);
//        UserGroupInformation.loginUserFromKeytab(principal, keytabFilename);
//      } catch (UnknownHostException e) {
//        throw new EsYarnAmException(
//            "Could not read localhost information for server principal construction; Bailing out...",
//            e);
//      } catch (IOException e) {
//        throw new EsYarnAmException("Could not log in.", e);
//      }
//    }
  }
  
  private void launchContainer(Container container) throws YarnException, IOException {
    ContainerLaunchContext ctx = Records
        .newRecord(ContainerLaunchContext.class);
    
    ctx.setEnvironment(setupEnv(para));
    ctx.setLocalResources(setupEsZipResource());
    ctx.setCommands(setupEsScript());
    
    log.info("About to launch container for command: " + ctx.getCommands());
    
    // setup container
    Map<String,ByteBuffer> startContainer = nmRpc
        .startContainer(container, ctx);
    log.info("Started container " + container);
  }
  
  private Map<String,String> setupEnv(ElatisticSearchPara appConfig) {
    // standard Hadoop env setup
    Map<String,String> env = YarnUtils.setupEnv(cfg);
  
    
  
     YarnUtils.addToEnv(env, "JAVA_HOME", para.getJavaHome());
    System.out.println("ENV:"+env.toString());
    return env;
  }
  
  private Map<String,LocalResource> setupEsZipResource() {
    // elasticsearch.zip
    Map<String,LocalResource> resources = new LinkedHashMap<String,LocalResource>();
    
    LocalResource esZip = Records.newRecord(LocalResource.class);
    String esZipHdfsPath = ElasticsearchInstallInfo.getPack();
//    String appSubmitterUserName = System
//        .getenv(ApplicationConstants.Environment.USER.name());
//    Path homePath = new Path("/user", appSubmitterUserName);
    
//    Path p = new Path(homePath, esZipHdfsPath);
    Path p = new Path(esZipHdfsPath);
    FileStatus fsStat;
    try {
      fsStat = FileSystem.get(cfg).getFileStatus(p);
    } catch (IOException ex) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot find Elasticsearch zip at [%s]; make sure the artifacts have been properly provisioned and the correct permissions are in place.",
              esZipHdfsPath), ex);
    }
    // use the normalized path as otherwise YARN chokes down the line
    esZip.setResource(ConverterUtils.getYarnUrlFromPath(fsStat.getPath()));
    esZip.setSize(fsStat.getLen());
    esZip.setTimestamp(fsStat.getModificationTime());
    esZip.setType(LocalResourceType.ARCHIVE);
    esZip.setVisibility(LocalResourceVisibility.PUBLIC);
    
    resources.put(p.getName(), esZip);
    
    // add ext arc ,for example jdk1.8 tar
    String[] extArcs = para.extArcs();
    if (extArcs != null) {
      for (String extArc : extArcs) {
        LocalResource extLr = Records.newRecord(LocalResource.class);
        Path pe = new Path(extArc);
        FileStatus fsState;
        try {
          fsState = FileSystem.get(cfg).getFileStatus(pe);
        } catch (IOException ex) {
          throw new IllegalArgumentException(
              String.format(
                  "Cannot find jar [%s]; make sure the artifacts have been properly provisioned and the correct permissions are in place.",
                  pe), ex);
        }
        // use the normalized path as otherwise YARN chokes down the line
        extLr.setResource(ConverterUtils.getYarnUrlFromPath(fsState.getPath()));
        extLr.setSize(fsState.getLen());
        extLr.setTimestamp(fsState.getModificationTime());
        extLr.setType(LocalResourceType.ARCHIVE);
        extLr.setVisibility(LocalResourceVisibility.PUBLIC);
        resources.put(fsState.getPath().getName(), extLr);
      }
    }
    
    return resources;
  }
  
  private List<String> setupEsScript() {
    List<String> cmds = new ArrayList<String>();
    // don't use -jar since it overrides the classpath
//    cmds.add("sleep 5000 \n");
    cmds.add("ulimit -a \n");
    cmds.add(YarnCompat.$$(ApplicationConstants.Environment.SHELL));
    // make sure to include the ES.ZIP archive name used in the local resource
    // setup above (since it's the folder where it got unpacked)
    cmds.add(ElasticsearchInstallInfo.getInstance().pack + "/" + ElasticsearchInstallInfo.getInstance().shell);
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
}