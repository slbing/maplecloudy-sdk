package com.maplecloudy.distribute.engine.appserver;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

public class NginxGatewayManager implements Runnable {
  
  static private String nginxPath = "/etc/nginx/conf.d";
  HashSet<String> hostInvolved = new HashSet<String>();
  
  private String logPath = "";
  private String ansiYml = "";
  
  @Override
  public void run() {
    
    hostInvolved.clear();
    try {
      updateNginxConfLocal();
      updateNginxConfRemote();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  public void Update() {
    
    Thread t = new Thread(this);
    t.start();
  }
  
  public void updateNginxConfLocal() throws IOException {
    
    for (NginxGateway ng : NginxGateway.arrAdd)
      hostInvolved.add(ng.hostAddress);
    for (NginxGateway ng : NginxGateway.arrDel)
      hostInvolved.add(ng.hostAddress);
    
    NginxGateway.mergeNginx();
    
    // delete hosts.conf with update
    for (String host : hostInvolved) {
      deleteFile(host + ".conf");
    }
    
    for (NginxGateway ng : NginxGateway.arrNg) {
      if (hostInvolved.contains(ng.hostAddress)) {
        
        File file = new File(ng.hostAddress + ".conf");
        if (!file.exists()) {
          
          file.createNewFile();
        }
        appendConf(ng.hostAddress + ".conf", ng);
      }
    }
    
  }
  
  public void appendConf(String fileName, NginxGateway ng)
      throws FileNotFoundException {
    PrintWriter printWriter = new PrintWriter(fileName);
    printWriter.append("server {\n" + " listen " + ng.portNginx + ";\n"
        + " access_log kang_kibana.log;\n" + "\n" + " location / {\n"
        + " proxy_pass \"" + ng.hostAddress + ":" + ng.hostAddress + "\";\n"
        + " proxy_set_header Host $host:7090;\n"
        + " proxy_set_header X-Real-IP $remote_addr;\n"
        + " proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;\n"
        + " proxy_set_header Via \"nginx\";\n" + " }\n" + "}"
    
    );
    printWriter.close();
  }
  
  public void updateNginxConfRemote() throws IOException {
    
    for (String host : hostInvolved) {
      
      File file = new File(host + ".conf");
      if (!file.exists()) {
        file.createNewFile();
      }
      createAnsibleYml(host);
      String cmd = assembleCmd(host + ".yml");
      runCmd(cmd);
    }
    // update checkInfo for each ng
  }
  
  public String assembleCmd(String yml) {
    
    return "ansible-playbook " + yml;
  }
  
  public void createAnsibleYml(String host) throws FileNotFoundException {
    PrintWriter printWriter = new PrintWriter(host+".yml");
    printWriter.append(
        "- hosts: kang \n" + " remote_user: root\n" + " gather_facts: no\n"
            + " tasks:\n" + " - name: copy config\n" + " copy:\n" + " src : "
            + host +".conf" + "\n" + " dest: /etc/nginx/conf.d/\n" + "\n"
            + " - service:\n" + " name: nginx\n" + " state: restarted"
    
    );
    printWriter.close();
  }
  
  public static boolean deleteFile(String fileName) {
    File file = new File(fileName);
    // 如果文件路径所对应的文件存在，并且是一个文件，则直接删除
    if (file.exists() && file.isFile()) {
      if (file.delete()) {
        System.out.println("删除单个文件" + fileName + "成功！");
        return true;
      } else {
        System.out.println("删除单个文件" + fileName + "失败！");
        return false;
      }
    } else {
      System.out.println("删除单个文件失败：" + fileName + "不存在！");
      return false;
    }
  }
  
  public void runCmd(String cmd) {
    
    try {
      Process amProc = Runtime.getRuntime().exec(cmd);
      
      final BufferedReader errReader = new BufferedReader(new InputStreamReader(
          amProc.getErrorStream(), Charset.forName("UTF-8")));
      final BufferedReader inReader = new BufferedReader(new InputStreamReader(
          amProc.getInputStream(), Charset.forName("UTF-8")));
      
      // read error and input streams as this would free up the buffers
      // free the error stream buffer
      Thread errThread = new Thread() {
        @Override
        public void run() {
          try {
            String line = errReader.readLine();
            while ((line != null) && !isInterrupted()) {
              System.err.println("cmd output------:" + line);
              line = errReader.readLine();
            }
          } catch (IOException ioe) {
            // LOG.warn("Error reading the error stream", ioe);
          }
        }
      };
      Thread outThread = new Thread() {
        @Override
        public void run() {
          try {
            String line = inReader.readLine();
            while ((line != null) && !isInterrupted()) {
              System.out.println("cmd output------:" + line);
              line = inReader.readLine();
            }
          } catch (IOException ioe) {
            // LOG.warn("Error reading the out stream", ioe);
          }
        }
      };
      try {
        errThread.start();
        outThread.start();
      } catch (IllegalStateException ise) {}
      
      try {
        int exitCode = amProc.waitFor();
        
      } catch (InterruptedException e) {
        e.printStackTrace();
      } finally {}
      
      try {
        // make sure that the error thread exits
        // on Windows these threads sometimes get stuck and hang the execution
        // timeout and join later after destroying the process.
        errThread.join();
        outThread.join();
        errReader.close();
        inReader.close();
      } catch (InterruptedException ie) {
        
      } catch (IOException ioe) {
        
      }
      amProc.destroy();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  public static void main(String args[]) {
    
  }
}
