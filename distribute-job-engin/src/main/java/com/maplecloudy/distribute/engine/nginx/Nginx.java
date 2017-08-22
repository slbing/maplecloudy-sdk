package com.maplecloudy.distribute.engine.nginx;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.Map;

import com.google.common.collect.Maps;
import com.maplecloudy.distribute.engine.utils.DeleteFileUtil;

public class Nginx {
  
  static String nginxRoot = "nginx";
  static String nginxPath = "/usr/local/webservices/nginx/conf/conf.d"; // demo.maplecloudy.com
  static String ansibleCmd = "ansible-playbook ";
//  static String ansibleCmd = "/usr/local/bin/ansible-playbook ";
  public static Map<String,NginxServer> nginxServers = Maps.newHashMap();
  public String name;
  
  // public void store() throws IOException {
  // File nginxF = new File(name);
  // for (Map.Entry<String,NginxApplication> entry : apps.entrySet()) {
  // if (!nginxF.exists()) nginxF.mkdirs();
  // File nginxConfF = new File(nginxF, entry.getKey());
  // Gson gson = new GsonBuilder().create();
  // FileWriter wr = new FileWriter(nginxConfF);
  // gson.toJson(entry.getValue(), wr);
  // wr.flush();
  // wr.close();
  // }
  // }
  //
  // public void load() throws JsonSyntaxException, JsonIOException,
  // FileNotFoundException {
  // apps.clear();
  // Gson gson = new GsonBuilder().create();
  // File nginxF = new File(name);
  // if (nginxF.exists()) {
  // File[] confs = nginxF.listFiles();
  // for (File conf : confs) {
  // NginxApplication nc = gson.fromJson(new FileReader(conf),
  // NginxApplication.class);
  // apps.put(conf.getName(), nc);
  // }
  // }
  // }
  
  public static void generateNginxConf() throws IOException {
    
    File nginxPath = new File(nginxRoot);
    
    for (Map.Entry<String,NginxServer> nginxServer : nginxServers.entrySet()) {
      if (!nginxPath.exists()) nginxPath.mkdirs();
      nginxServer.getValue().generateNginxConf(nginxRoot);
    }
  }
  
  public static synchronized NginxServer updateLocal(NginxGatewayPara para) {
    
    if (nginxServers.get(para.nginxIp) == null) {
      
      NginxServer nginxServer = new NginxServer();
      nginxServer.serverName = para.nginxIp;
      nginxServers.put(para.nginxIp, nginxServer);
      
    }
    nginxServers.get(para.nginxIp).updateLocal(para);
    
    return nginxServers.get(para.nginxIp);
  }
  
  public static synchronized boolean updateRemote() throws IOException {
    removeNginxConf();
    generateNginxConf();
    
    for (Map.Entry<String,NginxServer> server : nginxServers.entrySet()) {
      
      for (Map.Entry<String,NginxAppType> app : server.getValue().nginxApps
          .entrySet()) {
        String cmd = createAnsibleYml(server.getValue().serverName,
            app.getValue().appTypeName);
        runCmd(ansibleCmd + cmd);
      }
    }
    return true;
  }
  
  public static void removeNginxConf() {
    
    DeleteFileUtil.deleteDirectory(nginxRoot);
    
  }
  
  public static String createAnsibleYml(String nginxIp, String appType)
      throws FileNotFoundException {
    PrintWriter printWriter = new PrintWriter(
        "nginx/" + nginxIp + "/restart.yml");
    printWriter.append("- hosts: demo\n" + "  remote_user: root\n"
        + "  gather_facts: no\n" + "  tasks:\n" + "    - name: copy config\n"
        + "      copy:\n" + "       src : " + appType + ".conf" + "\n"
        + "       dest: " + nginxPath + "/\n" + "\n" + "    - service:\n"
        + "        name: nginx\n" + "        state: restarted\n"
    
    );
    printWriter.close();
    return "nginx/" + nginxIp + "/restart.yml";
  }
  
  public static void runCmd(String cmd) {
    
    try {
      Process amProc = Runtime.getRuntime().exec(cmd);
      
      final BufferedReader errReader = new BufferedReader(new InputStreamReader(
          amProc.getErrorStream(), Charset.forName("UTF-8")));
      final BufferedReader inReader = new BufferedReader(new InputStreamReader(
          amProc.getInputStream(), Charset.forName("UTF-8")));
      
      // read error and input streams as this would free up the buffers
      // free the error stream buffer
      Thread errThread = new CmdReader() {
        @Override
        public void run() {
          try {
            String line = errReader.readLine();
            while ((line != null) && !isInterrupted()) {
              System.err.println("cmd output------:" + line);
              if (line.contains("fail")) this.success = false;
              line = errReader.readLine();
              
            }
          } catch (IOException ioe) {
            System.out
                .println("Error reading the error stream:" + ioe.getMessage());
          }
        }
      };
      Thread outThread = new CmdReader() {
        @Override
        public void run() {
          try {
            String line = inReader.readLine();
            while ((line != null) && !isInterrupted()) {
              System.out.println("cmd output------:" + line);
              if (line.contains("fail")) this.success = false;
              line = inReader.readLine();
              
            }
          } catch (IOException ioe) {
            // LOG.warn("Error reading the out stream", ioe);
            System.out
                .println("Error reading the error stream:" + ioe.getMessage());
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
  
  public static void main(String args[]) throws IOException {
    NginxGatewayPara ngpara = new NginxGatewayPara();
    Nginx ng = new Nginx();
    ngpara.nginxId = "";
    ngpara.domain = "demo.maplecloudy.com";
    
    ngpara.nginxIp = "60.205.171.123";
    
    ngpara.appConf = "kibana01";
    
    ngpara.appHost = "10.0.4.1";
    ngpara.appPort = 62794;
    ngpara.proxyPort = 5553;
    
    ngpara.appType = "TEST";
    
    Nginx.updateLocal(ngpara);
    Nginx.updateRemote();
    
    // Nginx ng = new Nginx();
    // ng.name = "demo.maplecloudy.com";
    // ng.load();
    
  }
}

class CmdReader extends Thread {
  
  boolean success = true;
}
