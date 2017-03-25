package com.maplecloudy.distribute.engine;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import com.google.inject.Inject;

@Path("/ws/v1/maplecloudy")
public class MapleAppServices {
  private final AppContext appCtx;
  private final App app;
  
  private @Context HttpServletResponse response;
  
  @Inject
  public MapleAppServices(final App app, final AppContext context) {
    this.appCtx = context;
    this.app = app;
  }
  
  private void init() {
    // clear content type
    response.setContentType(null);
  }
  
  public static class Info {
    public String content = "hello world";
  }
  
  @GET
  @Path("/info")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public Info getAppInfo() {
    init();
    return new Info();
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
  
  public static void main(String[] args) {
    WebApps.$for("").at("0.0.0.0").with(new Configuration()).start(new WebApp() {
      @Override
      public void setup() {
        bind(GenericExceptionHandler.class);
        bind(MapleAppServices.class);
      }
    });
  }
}
