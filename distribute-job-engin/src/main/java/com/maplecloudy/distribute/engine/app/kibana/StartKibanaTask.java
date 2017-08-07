package com.maplecloudy.distribute.engine.app.kibana;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.google.common.collect.Lists;
import com.maplecloudy.distribute.engine.MapleCloudyEngineShellClient;
import com.maplecloudy.distribute.engine.appserver.AppPara;
import com.maplecloudy.distribute.engine.appserver.Nginx;
import com.maplecloudy.distribute.engine.appserver.NginxGateway;
import com.maplecloudy.distribute.engine.appserver.NginxGatewayPara;
import com.maplecloudy.distribute.engine.apptask.AppTask;

public class StartKibanaTask extends AppTask {
  
  public StartKibanaTask(AppPara para) {
    super(para);
    
  }
  
  @Override
  public void run() {
    try {
      checkInfo.clear();
      this.checkInfo.add("Start Task!");
      ApplicationId appid = this.checkTaskApp();
      if ( appid != null)
      {
        updateNginx(appid);
        return;
      }
      if (!this.checkEnv()) return;
      
      final KibanaPara kpara = (KibanaPara) this.para;
      List<String> cmds = Lists.newArrayList();
      cmds.add("-m");
      cmds.add("" + kpara.memory);
      cmds.add("-cpu");
      cmds.add("" + kpara.cpu);
      cmds.add("-sc");
      cmds.add(kpara.getScFile());
      cmds.add("-jar");
      cmds.add(kpara.getConfFile());
      cmds.add("-arc");
      cmds.add(KibanaInstallInfo.getPack());
      cmds.add("-args");
      cmds.add("sh kibana.sh");
      cmds.add("-type");
      cmds.add(this.para.getAppType());
      cmds.add("-damon");
      final String[] args = cmds.toArray(new String[cmds.size()]);
      
      final Configuration conf = this.getConf();
      UserGroupInformation ugi = UserGroupInformation
          .createProxyUser(this.para.user, UserGroupInformation.getLoginUser());
       appid = ugi.doAs(new PrivilegedAction<ApplicationId>() {
        @Override
        public ApplicationId run() {
          try {
            MapleCloudyEngineShellClient mcsc = new MapleCloudyEngineShellClient(
                new YarnConfiguration(conf));
            ApplicationId appid = mcsc.submitApp(args, kpara.getName());
            checkInfo
                .add("kibana submit yarn sucess,with application id:" + appid);
            return appid;
          } catch (Exception e) {
            e.printStackTrace();
            checkInfo.add("run kibana error with:" + e.getMessage());
            return null;
          }
          
        }
        
      });
      if (appid != null) {
        this.appids.add(appid);
        
        updateNginx(appid);
      }
      // checkInfo.add("yarn app have submit");
      
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  public void updateNginx(ApplicationId appid)
      throws YarnException, IOException, InterruptedException {
    boolean updateNginx = true;
    while (updateNginx) {
      YarnClient yarnClient = YarnClient.createYarnClient();
      
      yarnClient.init(new YarnConfiguration(this.getConf()));
      yarnClient.start();
      ApplicationReport report = yarnClient.getApplicationReport(appid);
      if (report.getYarnApplicationState() == YarnApplicationState.FAILED
          || report.getYarnApplicationState() == YarnApplicationState.FINISHED
          || report.getYarnApplicationState() == YarnApplicationState.KILLED) {
        checkInfo.add("App have run with appid:" + report.getApplicationId()
            + ", and finishedwith with status:"
            + report.getYarnApplicationState());
        updateNginx = false;
      } else if (report
          .getYarnApplicationState() == YarnApplicationState.RUNNING) {
        
        checkInfo.add("App have run with appid:" + report.getApplicationId()
            + ", now status" + report.getYarnApplicationState()
            + ",start update nginx!");
        
        NginxGatewayPara ngpara = new NginxGatewayPara();
        
        ngpara.nginxIp = this.para.nginxIp;
        String host = report.getHost();
        String[] hosts = host.split("/");
        if (hosts.length > 1) host = hosts[1];
        else if (hosts.length > 0) {
          host = hosts[0];
        }
        
        ngpara.appHost = host;
        ngpara.domain = para.nginxDomain;
        ngpara.appPort = this.getPort();
        ngpara.proxyPort = para.port;
        ngpara.nginxId = this.para.nginxIp;
        ngpara.appConf = this.para.appConf;
        ngpara.appType = this.para.getAppType();
        
        Nginx ng = Nginx.updateLocal(ngpara);
        ng.updateRemote();
        
        updateNginx = false;
        
        checkInfo.add("update nginx finished!");
      } else {
        checkInfo.add("App have run with appid:" + report.getApplicationId()
            + ", now status is::" + report.getYarnApplicationState());
      }
      Thread.sleep(5000);
    }
  }
  
  public boolean checkEnv() throws Exception {
    boolean bret = true;
    final Configuration conf = this.getConf();
    FileSystem fs = FileSystem.get(this.getConf());
    // check engint
    bret = checkEngine();
    if (fs.exists(new Path(KibanaInstallInfo.getPack()))) {
      checkInfo.add("kibana install is ok!");
    } else {
      checkInfo.add(KibanaInstallInfo.getPack() + " not exist!");
      bret = false;
    }
    
    KibanaPara kpara = (KibanaPara) this.para;
    checkInfo.add("GenerateConf with para.");
    final String confFile = kpara.GenerateConf(this.getPort());
    checkInfo.add("GenerateConf sucess: " + confFile);
    UserGroupInformation ugi = UserGroupInformation
        .createProxyUser(this.para.user, UserGroupInformation.getLoginUser());
    
    boolean bupload = ugi.doAs(new PrivilegedAction<Boolean>() {
      @Override
      public Boolean run() {
        try {
          FileSystem fs = FileSystem.get(conf);
          fs.copyFromLocalFile(false, true, new Path(confFile),
              new Path(confFile));
        } catch (IllegalArgumentException | IOException e) {
          e.printStackTrace();
          checkInfo
              .add("intall :" + confFile + " error with:" + e.getMessage());
          return false;
        }
        checkInfo.add("Install confile succes!");
        return true;
      }
      
    });
    if (!bupload) bret = bupload;
    checkInfo.add("Generate sc with para.");
    final String scFile = kpara.GenerateSc();
    checkInfo.add("Generate sc sucess: " + scFile);
    ugi = UserGroupInformation.createProxyUser(this.para.user,
        UserGroupInformation.getLoginUser());
    bupload = ugi.doAs(new PrivilegedAction<Boolean>() {
      @Override
      public Boolean run() {
        try {
          FileSystem fs = FileSystem.get(conf);
          fs.copyFromLocalFile(false, true, new Path(scFile), new Path(scFile));
        } catch (IllegalArgumentException | IOException e) {
          e.printStackTrace();
          checkInfo.add("intall :" + scFile + " error with:" + e.getMessage());
          return false;
        }
        checkInfo.add("Install scfile succes!");
        return true;
      }
      
    });
    if (!bupload) bret = bupload;
    
    // send update ngix
    return bret;
    
  }
  
  @Override
  public String getName() {
    return para.getName();
  }
  
}