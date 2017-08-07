package com.maplecloudy.distribute.engine.appserver;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

public class Nginx {
  
  static String nginxPath = "/usr/local/webservices/nginx/conf/conf.d"; // demo.maplecloudy.com
  static String ansibleCmd = "/usr/local/bin/ansible-playbook ";
  public static Map<String,Nginx> nginxs = Maps.newHashMap();
  public String name;
  
  public Map<String,NginxConf> apps = Maps.newHashMap();
  
  public void store() throws IOException {
    File nginxF = new File(name);
    for (Map.Entry<String,NginxConf> entry : apps.entrySet()) {
      if (!nginxF.exists()) nginxF.mkdirs();
      File nginxConfF = new File(nginxF, entry.getKey());
      Gson gson = new GsonBuilder().create();
      FileWriter wr = new FileWriter(nginxConfF);
      gson.toJson(entry.getValue(), wr);
      wr.flush();
      wr.close();
    }
  }
  
  public void load() throws JsonSyntaxException, JsonIOException,
      FileNotFoundException {
    apps.clear();
    Gson gson = new GsonBuilder().create();
    File nginxF = new File(name);
    if (nginxF.exists()) {
      File[] confs = nginxF.listFiles();
      for (File conf : confs) {
        NginxConf nc = gson.fromJson(new FileReader(conf), NginxConf.class);
        apps.put(conf.getName(), nc);
      }
    }
  }
  
  public void generateNginxConf() throws IOException {
    File nginxPath = new File("nginx/" + this.name);
    for (Map.Entry<String,NginxConf> entry : apps.entrySet()) {
      if (!nginxPath.exists()) nginxPath.mkdirs();
      File nginxConf = new File(nginxPath, entry.getKey() + ".conf");
      
      FileWriter wr = new FileWriter(nginxConf);
      for (Map.Entry<String,WebProxyServer> wps : entry.getValue().psm
          .entrySet()) {
        String content = wps.getValue().getStoreContent();
        wr.append(content);
      }
      wr.flush();
      wr.close();
    }
  }
  
  public static  boolean hasServer(NginxConf nc, NginxGatewayPara para) {
    
    for (Map.Entry<String,WebProxyServer> wps : nc.psm.entrySet()) {
      
      if (wps.getKey() == para.appConf
          && wps.getValue().appPort == para.appPort
          && wps.getValue().proxyPort == para.proxyPort) return true;
    }
    return false;
  }
  
  public static synchronized Nginx updateLocal(NginxGatewayPara para) {
    
    if (nginxs.get(para.nginxIp) == null) {
      
      Nginx nginx = new Nginx();
      nginx.name = para.nginxIp;
      nginxs.put(para.nginxIp, nginx);
      
    }
    if (nginxs.get(para.nginxIp).apps.get(para.appType) == null) {
      
      NginxConf nc = new NginxConf();
      nc.name = para.appType;
      nginxs.get(para.nginxIp).apps.put(para.appType, nc);
    }
    
    NginxConf nc = nginxs.get(para.nginxIp).apps.get(para.appType);
    if (!hasServer(nc, para)) {
      
      WebProxyServer wps = new WebProxyServer();
      wps.name = para.appConf;
      wps.appHost = para.appHost;
      wps.domain = para.domain;
      wps.appPort = para.appPort;
      wps.proxyPort = para.proxyPort;
      nc.psm.put(para.appConf, wps);
      
    }
    return nginxs.get(para.nginxIp);
  }
  
  public synchronized boolean updateRemote() throws IOException {
    this.removeNginxConf();
    this.generateNginxConf();
    
    for (Map.Entry<String,NginxConf> entry : apps.entrySet()) {
      String cmd = createAnsibleYml(this.name, entry.getValue().name);
      runCmd(ansibleCmd + cmd);
    }
    return true;
  }
  
  public void removeNginxConf() {
    
  }
  
  public String createAnsibleYml(String nginxIp, String appType)
      throws FileNotFoundException {
    PrintWriter printWriter = new PrintWriter("nginx/" + this.name
        + "/restart.yml");
    printWriter.append("- hosts: demo\n" + "  remote_user: root\n"
        + "  gather_facts: no\n" + "  tasks:\n" + "    - name: copy config\n"
        + "      copy:\n" + "       src : " + appType + ".conf" + "\n"
        + "       dest: " + nginxPath + "/\n" + "\n" + "    - service:\n"
        + "        name: nginx\n" + "        state: restarted\n"
    
    );
    printWriter.close();
    return "nginx/" + this.name + "/restart.yml";
  }
  
  public void runCmd(String cmd) {
    
    try {
      Process amProc = Runtime.getRuntime().exec(cmd);
      
      final BufferedReader errReader = new BufferedReader(
          new InputStreamReader(amProc.getErrorStream(),
              Charset.forName("UTF-8")));
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
              line = errReader.readLine();
              if(line.contains("fail")) this.success=false;
            }
          } catch (IOException ioe) {
            // LOG.warn("Error reading the error stream", ioe);
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
              
              line = inReader.readLine();
              if(line.contains("fail")) this.success=false;
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
  
  public static void main(String args[]) throws IOException {
    NginxGatewayPara ngpara = new NginxGatewayPara();
    Nginx ng = new Nginx();
    ngpara.appConf = "kibana01";
    ngpara.nginxId = "";
    ngpara.domain = "demo.maplecloudy.com";
    ngpara.nginxIp = "60.205.171.123";
    
    ngpara.appHost = "10.0.4.1";
    ngpara.domain = "";
    ngpara.appPort = 62794;
    ngpara.proxyPort = 5553;
    
    ngpara.appType = "kibana";
    
    ng = Nginx.updateLocal(ngpara);
    ng.updateRemote();
    
    // Nginx ng = new Nginx();
    // ng.name = "demo.maplecloudy.com";
    // ng.load();
    
  }
}

class CmdReader extends Thread {
  
  boolean success = true;
}

