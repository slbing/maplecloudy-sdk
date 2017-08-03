package com.maplecloudy.distribute.engine.app.kibana;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.google.common.collect.Lists;
import com.maplecloudy.distribute.engine.MapleCloudyEngineShellClient;
import com.maplecloudy.distribute.engine.app.engine.EngineInstallInfo;
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
      this.checkEnv();
      
      KibanaPara kpara = (KibanaPara) this.para;
      List<String> cmds = Lists.newArrayList();
      cmds.add("-sc");
      cmds.add(kpara.getSc());
      cmds.add("-jar");
      cmds.add(kpara.getConfFile());
      cmds.add("-arc");
      cmds.add(KibanaInstallInfo.getPack());
      cmds.add("-args");
      cmds.add("'sh kibana.sh'");
      cmds.add("-damon");
      final String[] args = cmds.toArray(new String[cmds.size()]);
      
      final Configuration conf = this.getConf();
      UserGroupInformation ugi = UserGroupInformation.createProxyUser(
          this.para.user, UserGroupInformation.getLoginUser());
      ugi.doAs(new PrivilegedAction<Boolean>() {
        @Override
        public Boolean run() {
          try {
            
            ToolRunner.run(new MapleCloudyEngineShellClient(
                new YarnConfiguration(conf)), args);
          } catch (Exception e) {
            e.printStackTrace();
            checkInfo.add("run kibana error with:" + e.getMessage());
            return false;
          }
          checkInfo.add("kibana submit yarn sucess,with application id:");
          return true;
        }
        
      });
    } catch (Exception e) {
      
    }
    
  }
  
  public boolean checkEnv() throws Exception {
    boolean bret = true;
    checkInfo.clear();
    
    final FileSystem fs = FileSystem.get(this.getConf());
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
    final String scFile = kpara.GenerateConf();
    checkInfo.add("Generate sc sucess: " + scFile);
    ugi = UserGroupInformation.createProxyUser(this.para.user,
        UserGroupInformation.getLoginUser());
    bret = ugi.doAs(new PrivilegedAction<Boolean>() {
      @Override
      public Boolean run() {
        try {
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
  
  @Override
  public String getName() {
    return para.getName();
  }
  
}