package com.maplecloudy.distribute.engine.appserver;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.Charset;

public class NgixGateway {

  private String confPath = "";
  private String ansiYml = "";
  private String portNginx; // nginx listen port
  private String hostAddress; // application host
  private String portApp; // app real port at its host
  private String logPath = "";
  private String cmd;
  
  static private String nginxPath = "/etc/nginx/conf.d";
  
  public String getConfName(String args[]) {
    
    return confPath;
  }
  
  public void assembleCmd(){
    
    cmd = "ansible-playbook " + ansiYml;
  }
  
  public void createConf() throws Exception {
    PrintWriter printWriter = new PrintWriter(confPath);
    printWriter.append(
        "server {\n" 
        + "    listen       "+portNginx+";\n"
        + "    access_log  kang_kibana.log;\n" + "\n"
        + "    location / {\n"
        + "      proxy_pass \""+hostAddress+":"+portApp+"\";\n"
        + "  proxy_set_header Host $host:7090;\n"
        + "      proxy_set_header X-Real-IP $remote_addr;\n"
        + "      proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;\n"
        + "      proxy_set_header Via \"nginx\";\n" 
        + "    }\n" 
        + "}"
    
    );
    printWriter.close();
    
  }
  
  public void createAnsibleYml() throws FileNotFoundException
  { 
    PrintWriter printWriter = new PrintWriter(ansiYml);
    printWriter.append(
          "- hosts: kang \n"
          +"  remote_user: root\n"
          +"  gather_facts: no\n"
          +"  tasks:\n"
          +"    - name: copy config\n"
          +"      copy:\n"
          +"        src : "+confPath+"\n"
          +"        dest: /etc/nginx/conf.d/\n"
          +"\n"
          +"    - service:\n"
          +"        name: nginx\n"
          +"        state: restarted" 
          
      );
    printWriter.close();
  }
  
  public void runCmd() {
   
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
  
  public static void main(String args[]) throws IOException {
    
    String cmd = "ansible-playbook ~/.maple/engine/ansible/restartnginx.yml";
    cmd = "pwd";
    NgixGateway ng = new NgixGateway();
    
  }
  
}