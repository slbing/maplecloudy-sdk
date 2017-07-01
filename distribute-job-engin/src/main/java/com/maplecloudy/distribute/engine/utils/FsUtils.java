package com.maplecloudy.distribute.engine.utils;

import java.io.IOException;
import java.security.PrivilegedAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

public class FsUtils {
  
  public static void main(final String[] args) throws Exception {
    String user = "maplecloudy";
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-user")) {
        i++;
        if (i < args.length) {
          user = args[i];
        }
      }
    }
  }
  
  public static void copyFromLocal(final String src, final String dst,
      String user) throws IOException {
    
    System.out.println(
        "copyFromLocal login in user:" + UserGroupInformation.getLoginUser());
    System.out.println("do as:" + user);
    System.out.println("copyFromLocal source:" + src);
    System.out.println("copyFromLocal destination:" + dst);
    
    UserGroupInformation ugi = UserGroupInformation.createProxyUser(user,
        UserGroupInformation.getLoginUser());
    ugi.doAs(new PrivilegedAction<Void>() {
      @Override
      public Void run() {
        String bak = dst + src.substring(src.indexOf("target") + 6) + ".bak";
        String bin = dst + src.substring(src.indexOf("target") + 6);
        
        System.out.println("src:" + src);
        System.out.println("dst:" + dst);
        System.out.println("bak:" + bak);
        System.out.println("bin:" + bin);
        Path srcPath = new Path(src);
        Path dstPath = new Path(dst);
        Path binPath = new Path(bin);
        Path bakPath = new Path(bak);
        try {
          
          Configuration conf = new Configuration();
          
          FileSystem fs = FileSystem.get(conf);
          if (!fs.exists(dstPath)) {
            fs.mkdirs(dstPath);
          }
          
          if (fs.exists(binPath)) {
            fs.rename(binPath, bakPath);
          }
          fs.copyFromLocalFile(srcPath, dstPath);
          
          if (fs.exists(bakPath)) {
            fs.delete(bakPath, true);
          }
          
          fs.close();
        } catch (Exception e) {
          Configuration conf = new Configuration();
          FileSystem fs;
          try {
            fs = FileSystem.get(conf);
            if (fs.exists(binPath)) {
              fs.delete(binPath, true);
            }
            if (fs.exists(bakPath)) {
              fs.rename(bakPath, binPath);
            }
          } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
          }
          
          e.printStackTrace();
          System.exit(-1);
        }
        return null;
      }
      
    });
  }
  
}
