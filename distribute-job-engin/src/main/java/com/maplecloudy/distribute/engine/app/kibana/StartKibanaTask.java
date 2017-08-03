package com.maplecloudy.distribute.engine.app.kibana;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.google.common.collect.Lists;
import com.maplecloudy.distribute.engine.MapleCloudyEngineShellClient;
import com.maplecloudy.distribute.engine.appserver.AppPara;
import com.maplecloudy.distribute.engine.apptask.AppTask;

public class StartKibanaTask extends AppTask {
  
  public StartKibanaTask(AppPara para) {
    super(para);
    
  }
  
  @Override
  public void run() {
    try {
      
      this.checkInfo.add("Start Task!");
      if(this.checkTaskApp())
        return;
      this.checkEnv();
      
      final KibanaPara kpara = (KibanaPara) this.para;
      List<String> cmds = Lists.newArrayList();
      cmds.add("-sc");
      cmds.add(kpara.getScFile());
      cmds.add("-jar");
      cmds.add(kpara.getConfFile());
      cmds.add("-arc");
      cmds.add(KibanaInstallInfo.getPack());
      cmds.add("-args");
      cmds.add("sh kibana.sh");
      cmds.add("-damon");
      final String[] args = cmds.toArray(new String[cmds.size()]);
      
      final Configuration conf = this.getConf();
      UserGroupInformation ugi = UserGroupInformation.createProxyUser(
          this.para.user, UserGroupInformation.getLoginUser());
      appid = ugi.doAs(new PrivilegedAction<ApplicationId>() {
        @Override
        public ApplicationId run() {
          try {
            MapleCloudyEngineShellClient mcsc = new MapleCloudyEngineShellClient(
                new YarnConfiguration(conf));
            ApplicationId appid = mcsc.submitApp(args, kpara.getName());
            checkInfo.add("kibana submit yarn sucess,with application id:"
                + appid);
            return appid;
          } catch (Exception e) {
            e.printStackTrace();
            checkInfo.add("run kibana error with:" + e.getMessage());
            return null;
          }
          
        }
        
      });
      System.out.println(checkInfo.toString());
    } catch (Exception e) {
      e.printStackTrace();
    }
    
  }
  
  public boolean checkEnv() throws Exception {
    boolean bret = true;
    checkInfo.clear();
    
     FileSystem fs = FileSystem.get(this.getConf());
     final Configuration conf = this.getConf();
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
    final String confFile = kpara.GenerateConf();
    checkInfo.add("GenerateConf sucess: " + confFile);
    UserGroupInformation ugi = UserGroupInformation.createProxyUser(
        this.para.user, UserGroupInformation.getLoginUser());
    bret = ugi.doAs(new PrivilegedAction<Boolean>() {
      @Override
      public Boolean run() {
        try {
          
         Path a = new Path(confFile);

         FileSystem fs = FileSystem.get(conf);
         
          fs.copyFromLocalFile(false, true, new Path(confFile), new Path(
              confFile));
        } catch (IllegalArgumentException | IOException e) {
          e.printStackTrace();
          checkInfo.add("intall :" + confFile + " error with:" + e.getMessage());
          return false;
        }
        checkInfo.add("Install confile succes!");
        return true;
      }
      
    });
    
    checkInfo.add("Generate sc with para.");
    final String scFile = kpara.GenerateSc();
    checkInfo.add("Generate sc sucess: " + scFile);
    ugi = UserGroupInformation.createProxyUser(this.para.user,
        UserGroupInformation.getLoginUser());
    bret = ugi.doAs(new PrivilegedAction<Boolean>() {
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
    
    return bret;
  }
  
  public boolean checkTaskApp() throws YarnException, IOException
  {
    boolean bret = false;
    YarnClient yarnClient = YarnClient.createYarnClient();
    Configuration conf = new YarnConfiguration(this.getConf());
    yarnClient.init(conf);
    yarnClient.start();
    List<ApplicationReport> reports = yarnClient.getApplications(Collections.singleton("MAPLECLOUDY-APP"));
    for(ApplicationReport report : reports)
    {
      if(report.getName().equals(this.getName()))
      {
        checkInfo.add("App has run with appid:"+report.getApplicationId());
        bret = true;
      }
      
    }
    return bret;
  }
  @Override public String getName() {
    return para.getName();
  }
  
}