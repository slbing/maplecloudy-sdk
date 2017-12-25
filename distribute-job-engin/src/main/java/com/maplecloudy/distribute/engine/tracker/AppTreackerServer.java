package com.maplecloudy.distribute.engine.tracker;

import org.apache.hadoop.http.HttpServer2;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

import com.sun.jersey.spi.container.servlet.ServletContainer;

public class AppTreackerServer {
  String addr = "0.0.0.0";
  int port = 0;
  
  public AppTreackerServer(String addr, int port) {
    if (addr != null) this.addr = addr;
    if (port >= 0) this.port = port;
  }
  
  private void start() throws Exception {
    
    Server server = new Server(port);
    
    Context context = new Context();
    context.setContextPath("/");
    server.setHandler(context);
    ServletHolder sh = new ServletHolder(ServletContainer.class);
    sh.setInitParameter("com.sun.jersey.config.property.resourceConfigClass",
        "com.sun.jersey.api.core.PackagesResourceConfig");
    sh.setInitParameter("com.sun.jersey.config.property.packages",
        "com.maplecloudy.distribute.engine.restful");
    context.addServlet(sh, "/*");
    server.start();
    System.out.println("port:"+server.getConnectors()[0].getLocalPort());
  }
  
  public static void main(String[] args) throws Exception {
    AppTreackerServer server = new AppTreackerServer(null, 7099);
    server.start();
  }
}