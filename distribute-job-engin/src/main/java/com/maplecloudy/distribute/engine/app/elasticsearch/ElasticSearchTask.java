package com.maplecloudy.distribute.engine.app.elasticsearch;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.google.common.collect.Lists;
import com.maplecloudy.distribute.engine.MapleCloudyEngineShellClient;
import com.maplecloudy.distribute.engine.appserver.AppPara;
import com.maplecloudy.distribute.engine.apptask.AppTask;
import com.maplecloudy.distribute.engine.utils.EngineUtils;

public class ElasticSearchTask extends AppTask {
  
  
  public ElasticSearchTask(AppPara para) {
    super(para);
    
  }
  
  @Override
  public void run() {
    try {
      checkInfo.clear();
      this.checkInfo.add("Start Task!");
      if (this.checkTaskApp()) return;
      if (!this.checkEnv()) return;
      
      final ElatisticSearchPara kpara = (ElatisticSearchPara) this.para;
      List<String> cmds = Lists.newArrayList();
      cmds.add("-m");
      cmds.add(""+kpara.memory);
      cmds.add("-cpu");
      cmds.add(""+kpara.cpu);
      cmds.add("-sc");
      cmds.add(kpara.getScFile());
      cmds.add("-jar");
      cmds.add(kpara.getConfFile());
      cmds.add("-arc");
      cmds.add(ElasticsearchInstallInfo.getPack());
      cmds.add("-args");
      cmds.add("sh kibana.sh");
      cmds.add("-type");
      cmds.add(this.para.getAppType());
      cmds.add("-damon");
      final String[] args = cmds.toArray(new String[cmds.size()]);
      
      final Configuration conf = this.getConf();
      UserGroupInformation ugi = UserGroupInformation.createProxyUser(
          this.para.user, UserGroupInformation.getLoginUser());
      ApplicationId appid = ugi.doAs(new PrivilegedAction<ApplicationId>() {
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
      if(appid != null)
        this.appids.add(appid);
      // checkInfo.add("yarn app have submit");
    } catch (Exception e) {
      e.printStackTrace();
    }
    
  }
  
  public boolean checkEnv() throws Exception {
    boolean bret = true;
    final Configuration conf = this.getConf();
    FileSystem fs = FileSystem.get(this.getConf());
    // check engint
    bret = checkEngine();
    if (fs.exists(new Path(ElasticsearchInstallInfo.getPack()))) {
      checkInfo.add("kibana install is ok!");
    } else {
      checkInfo.add(ElasticsearchInstallInfo.getPack() + " not exist!");
      bret = false;
    }
    
    ElatisticSearchPara kpara = (ElatisticSearchPara) this.para;
    checkInfo.add("GenerateConf with para.");
    final String confFile = kpara.GenerateConf(port);
    checkInfo.add("GenerateConf sucess: " + confFile);
    UserGroupInformation ugi = UserGroupInformation.createProxyUser(
        this.para.user, UserGroupInformation.getLoginUser());
    
    boolean bupload = ugi.doAs(new PrivilegedAction<Boolean>() {
      @Override
      public Boolean run() {
        try {
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
    
    //send update ngix
    return bret;
    
  }
  
  @Override
  public String getName() {
    return para.getName();
  }
  
 
}