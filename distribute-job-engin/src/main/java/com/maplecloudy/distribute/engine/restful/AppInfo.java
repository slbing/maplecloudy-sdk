package com.maplecloudy.distribute.engine.restful;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

@Path("/ws/v1/maplecloudyapp")
@XmlRootElement(name = "info")
@XmlAccessorType(XmlAccessType.FIELD)
public class AppInfo {
  public String id;
  public String log;
  
  @GET
  @Produces({MediaType.APPLICATION_JSON})
  public AppInfo getInfo() {
    AppInfo info = new AppInfo();
    info.id = "123123";
    info.log = "this log !!!";
    return info;
  }
  
  LogInfo loginfo = new LogInfo();
  
  @GET
  @Path("/log/{containerid}/{appowner}/{nodeid}")
  @Produces({MediaType.APPLICATION_JSON})
  public LogInfo getLog(@Context HttpServletRequest hsr,
      @PathParam("containerid") String cid,
      @PathParam("appowner") String appowner, @PathParam("nodeid") String nid,
      @DefaultValue("-4096") @QueryParam("start") int start,
      @QueryParam("end") int end) {
    loginfo.error.add("cid="+cid);
    loginfo.error.add("appowner="+appowner);
    loginfo.error.add("nodeid"+nid);
    loginfo.error.add("start="+start);
    loginfo.error.add("end="+end);
    // ContainerId containerId = verifyAndGetContainerId(cid);
    // NodeId nodeId = verifyAndGetNodeId(nid);
    // String appOwner = verifyAndGetAppOwner(appowner);
    // LogLimits logLimits = verifyAndGetLogLimits();
    // if (containerId == null || nodeId == null || appOwner == null
    // || appOwner.isEmpty() || logLimits == null) {
    // return this;
    // }
    //
    // ApplicationId applicationId = containerId.getApplicationAttemptId()
    // .getApplicationId();
    //
    //
    // String nmApplicationLogUrl = getApplicationLogURL(applicationId);
    // if (!conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
    // YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED)) {
    // html.h1()
    // ._("Aggregation is not enabled. Try the nodemanager at " + nodeId)
    // ._();
    // if (nmApplicationLogUrl != null) {
    // html.h1()._("Or see application log at " + nmApplicationLogUrl)._();
    // }
    // return;
    // }
    //
    // Path remoteRootLogDir = new Path(conf.get(
    // YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
    // YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR));
    // Path remoteAppDir = LogAggregationUtils.getRemoteAppLogDir(
    // remoteRootLogDir, applicationId, appOwner,
    // LogAggregationUtils.getRemoteNodeLogDirSuffix(conf));
    // RemoteIterator<FileStatus> nodeFiles;
    // try {
    // Path qualifiedLogDir = FileContext.getFileContext(conf).makeQualified(
    // remoteAppDir);
    // nodeFiles = FileContext.getFileContext(qualifiedLogDir.toUri(), conf)
    // .listStatus(remoteAppDir);
    // } catch (FileNotFoundException fnf) {
    // html.h1()
    // ._("Logs not available for " + logEntity
    // + ". Aggregation may not be complete, "
    // + "Check back later or try the nodemanager at " + nodeId)._();
    // if (nmApplicationLogUrl != null) {
    // html.h1()._("Or see application log at " + nmApplicationLogUrl)._();
    // }
    // return;
    // } catch (Exception ex) {
    // html.h1()._("Error getting logs at " + nodeId)._();
    // return;
    // }
    //
    // boolean foundLog = false;
    // String desiredLogType = $(CONTAINER_LOG_TYPE);
    // try {
    // while (nodeFiles.hasNext()) {
    // AggregatedLogFormat.LogReader reader = null;
    // try {
    // FileStatus thisNodeFile = nodeFiles.next();
    // if (thisNodeFile.getPath().getName().equals(applicationId + ".har")) {
    // Path p = new Path("har:///"
    // + thisNodeFile.getPath().toUri().getRawPath());
    // nodeFiles = HarFs.get(p.toUri(), conf).listStatusIterator(p);
    // continue;
    // }
    // if (!thisNodeFile.getPath().getName()
    // .contains(LogAggregationUtils.getNodeString(nodeId))
    // || thisNodeFile.getPath().getName()
    // .endsWith(LogAggregationUtils.TMP_FILE_SUFFIX)) {
    // continue;
    // }
    // long logUploadedTime = thisNodeFile.getModificationTime();
    // reader = new AggregatedLogFormat.LogReader(conf,
    // thisNodeFile.getPath());
    //
    // String owner = null;
    // Map<ApplicationAccessType,String> appAcls = null;
    // try {
    // owner = reader.getApplicationOwner();
    // appAcls = reader.getApplicationAcls();
    // } catch (IOException e) {
    // LOG.error("Error getting logs for " + logEntity, e);
    // continue;
    // }
    // ApplicationACLsManager aclsManager = new ApplicationACLsManager(conf);
    // aclsManager.addApplication(applicationId, appAcls);
    //
    // String remoteUser = request().getRemoteUser();
    // UserGroupInformation callerUGI = null;
    // if (remoteUser != null) {
    // callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    // }
    // if (callerUGI != null
    // && !aclsManager.checkAccess(callerUGI,
    // ApplicationAccessType.VIEW_APP, owner, applicationId)) {
    // html.h1()
    // ._("User [" + remoteUser
    // + "] is not authorized to view the logs for " + logEntity
    // + " in log file [" + thisNodeFile.getPath().getName() + "]")
    // ._();
    // LOG.error("User [" + remoteUser
    // + "] is not authorized to view the logs for " + logEntity);
    // continue;
    // }
    //
    // AggregatedLogFormat.ContainerLogsReader logReader = reader
    // .getContainerLogsReader(containerId);
    // if (logReader == null) {
    // continue;
    // }
    //
    // foundLog = readContainerLogs(html, logReader, logLimits,
    // desiredLogType, logUploadedTime);
    // } catch (IOException ex) {
    // LOG.error("Error getting logs for " + logEntity, ex);
    // continue;
    // } finally {
    // if (reader != null) reader.close();
    // }
    // }
    // if (!foundLog) {
    // if (desiredLogType.isEmpty()) {
    // html.h1("No logs available for container " + containerId.toString());
    // } else {
    // html.h1("Unable to locate '" + desiredLogType
    // + "' log for container " + containerId.toString());
    // }
    // }
    // } catch (IOException e) {
    // html.h1()._("Error getting logs for " + logEntity)._();
    // LOG.error("Error getting logs for " + logEntity, e);
    // }
    return loginfo;
  }
  
  private ContainerId verifyAndGetContainerId(String cid) {
    
    if (cid == null || cid.isEmpty()) {
      loginfo.error.add("Cannot get container logs without a ContainerId");
      return null;
    }
    ContainerId containerId = null;
    try {
      containerId = ConverterUtils.toContainerId(cid);
    } catch (IllegalArgumentException e) {
      
      loginfo.error.add("Cannot get container logs for invalid containerId: "
          + cid);
      return null;
    }
    return containerId;
  }
  
  private NodeId verifyAndGetNodeId(String nodeIdStr) {
    
    if (nodeIdStr == null || nodeIdStr.isEmpty()) {
      loginfo.error.add("Cannot get container logs without a NodeId");
      return null;
    }
    NodeId nodeId = null;
    try {
      nodeId = ConverterUtils.toNodeId(nodeIdStr);
    } catch (IllegalArgumentException e) {
      loginfo.error.add("Cannot get container logs. Invalid nodeId: "
          + nodeIdStr);
      return null;
    }
    return nodeId;
  }
  
  private String verifyAndGetAppOwner(String appOwner) {
    
    if (appOwner == null || appOwner.isEmpty()) {
      loginfo.error.add("Cannot get container logs without an app owner");
    }
    return appOwner;
  }
  
  private static class LogLimits {
    long start;
    long end;
  }
  
  private LogLimits verifyAndGetLogLimits(long start, long end) {
    LogLimits limits = new LogLimits();
    limits.start = start;
    limits.end = end;
    return limits;
  }
  
  private String getApplicationLogURL(ApplicationId applicationId, String nodeId) {
    String appId = applicationId.toString();
    if (appId == null || appId.isEmpty()) {
      return null;
    }
    if (nodeId == null || nodeId.isEmpty()) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    String scheme = YarnConfiguration.useHttps(new YarnConfiguration()) ? "https://"
        : "http://";
    sb.append(scheme).append(nodeId).append("/node/application/").append(appId);
    return sb.toString();
  }
  
  // @GET
  // @Path("/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}")
  // @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  // public TaskAttemptInfo getJobTaskAttemptId(@Context HttpServletRequest hsr,
  // @PathParam("jobid") String jid, @PathParam("taskid") String tid,
  // @PathParam("attemptid") String attId) {
  //
  // init();
  // Job job = getJobFromJobIdString(jid, appCtx);
  // checkAccess(job, hsr);
  // Task task = getTaskFromTaskIdString(tid, job);
  // TaskAttempt ta = getTaskAttemptFromTaskAttemptString(attId, task);
  // if (task.getType() == TaskType.REDUCE) {
  // return new ReduceTaskAttemptInfo(ta, task.getType());
  // } else {
  // return new TaskAttemptInfo(ta, task.getType(), true);
  // }
  // }
  
}
