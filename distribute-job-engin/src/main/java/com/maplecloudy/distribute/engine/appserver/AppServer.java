package com.maplecloudy.distribute.engine.appserver;

import java.io.IOException;

import org.apache.avro.ipc.HttpServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.reflect.ReflectResponder;
import org.apache.avro.reflect.ReflectData;

public class AppServer {
  
  private static Server server = null;
  
  private static void startServer(int port) throws IOException {
    server = new HttpServer(new ReflectResponder(IApp.class, new AppImpl()), port);
    server.start();
    // the server implements the Mail protocol (MailImpl)
  }
  
  public static void main(String[] args) throws IOException {
//    System.out.println(ReflectData.get().getSchema(String.class));
    int port = 9002;
    if (args.length > 0)
    
    port = Integer.parseInt(args[0]);
    System.out.println("Starting server");
    // usually this would be another app, but for simplicity
    startServer(port);
    System.out.println("Server started");
    
    System.out.println(ReflectData.get().getProtocol(IApp.class));
  }
}