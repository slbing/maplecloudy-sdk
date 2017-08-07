package com.maplecloudy.distribute.engine.appserver;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.HashSet;

import com.maplecloudy.distribute.engine.apptask.AppTask;

public class NginxGatewayManager extends AppTask {
  
  public static final int REFRESH_RIME = 60000;
  public NginxGatewayManager(AppPara para) {
    super(para);
    // TODO Auto-generated constructor stub
  }
  
  static private String nginxPath = "/usr/local/webservices/nginx/conf/conf.d"; //demo.maplecloudy.com
  HashSet<String> hostInvolved = new HashSet<String>();
  
  private String logPath = "";
  private String ansiYml = "";
  public boolean brun = true;

  
  @Override
  public void run() {
    
    while (brun) {
      
    
      hostInvolved.clear();
      try {
        updateNginxConfLocal();
        updateNginxConfRemote();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      
      try {
        Thread.sleep(60000);
      } catch (InterruptedException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
    }
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
    
    for (int i = 0; i < NginxGateway.arrNg.size(); i++) {
      
      NginxGateway ng = NginxGateway.arrNg.get(i);
      
      if (hostInvolved.contains(ng.hostAddress)) {
        
        File file = new File(ng.hostAddress + ".conf");
        if (!file.exists()) {
          
          file.createNewFile();
        }
        appendConf(ng.hostAddress + ".conf", ng);
      }
    }
    
  }
  
  public void updateNigx(AppPara para)
  {
    String confFile = "nginx/"+para.getAppType().toLowerCase()+".conf";
    
  }
  
  public void appendConf(String fileName, NginxGateway ng)
      throws IOException {
    FileWriter writer = new FileWriter(fileName, true);
    
    String content = "server {\n" + " listen " + ng.portNginx + ";\n"
        + " access_log kang_kibana.log;\n" + "\n" + " location / {\n"
        + " proxy_pass \"http://" + ng.hostAddress + ":" + ng.portApp + "\";\n"
        + " proxy_set_header Host $host:7090;\n"
        + " proxy_set_header X-Real-IP $remote_addr;\n"
        + " proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;\n"
        + " proxy_set_header Via \"nginx\";\n" + " }\n" + "}\n\n";
    
    writer.write(content);
    writer.close();
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
    
    return "/usr/local/bin/ansible-playbook " + yml;
  }
  
  public void createAnsibleYml(String host) throws FileNotFoundException {
    PrintWriter printWriter = new PrintWriter(host + ".yml");
    printWriter.append("- hosts: demo\n" + "  remote_user: root\n"
        + "  gather_facts: no\n" + "  tasks:\n" + "    - name: copy config\n"
        + "      copy:\n" + "       src : " + host + ".conf" + "\n"
        + "       dest: "+nginxPath+"/\n" + "\n" + "    - service:\n"
        + "        name: nginx\n" + "        state: restarted\n"
    
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
    
//    NginxGateway ng = new NginxGateway();
//    ng.id = "1";
//    ng.confPath = "";
//    ng.portNginx = "5551";
//    ng.hostAddress = "10.0.4.1";
//    ng.portApp = "62794";
//    NginxGateway.addNginx(ng);
//
//    ng = new NginxGateway();
//    ng.id = "1";
//    ng.confPath = "";
//    ng.portNginx = "5552";
//    ng.hostAddress = "10.0.4.1";
//    ng.portApp = "62794";
//    NginxGateway.addNginx(ng);
//    
//    
//    NginxGatewayManager ngm = new NginxGatewayManager(new NginxGatewayPara());
//    ngm.run();
    
    // ngm.runCmd("/usr/local/bin/ansible-playbook ");
    // ngm.runCmd("ls");
  }
  
  @Override
  public String getName() {
    // TODO Auto-generated method stub
    return null;
  }
}
