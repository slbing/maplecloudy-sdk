package com.maplecloudy.distribute.engine.apptask;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;

import com.maplecloudy.distribute.engine.MapleCloudyEngineClient;
import com.maplecloudy.distribute.engine.app.engine.EngineInstallInfo;
import com.maplecloudy.distribute.engine.appserver.AppPara;

public abstract class AppTask extends Configured implements Runnable{
  public AppPara para;
  public AppTask(AppPara para)
  {
    this.para = para;
    Configuration conf = new Configuration();
    this.para.setConf(conf);
    this.setConf(conf);
  }
  public abstract String getName();
  
  public  List<String> checkInfo = new ArrayList<String>();
  public boolean checkEngine() throws IOException
  {
    boolean bret = true;
    final FileSystem fs = FileSystem.get(getConf());
    if(fs.exists(new Path(EngineInstallInfo.getPack())))
    {
      checkInfo.add("Maplecloudy Engine install ok!");
    }
    else
    {
      checkInfo.add("Try install Maplecloudy engine!");
      UserGroupInformation ugi = UserGroupInformation.createProxyUser("maplecloudy",
          UserGroupInformation.getLoginUser());
      bret = ugi.doAs(new PrivilegedAction<Boolean>() {
        @Override
        public Boolean run() {
          try {
            fs.copyFromLocalFile(false, true, new Path("lib/"+EngineInstallInfo.pack), new Path(EngineInstallInfo.getPack()));
          } catch (IllegalArgumentException | IOException e) {
            e.printStackTrace();
            checkInfo.add("Install Maplecloudy engine faild whit:"+e.getMessage());
           return false;
          }
          checkInfo.add("Install Maplecloudy engine succes!");
          return true;
        }
        
      });
    }
    return bret;
  }
}
