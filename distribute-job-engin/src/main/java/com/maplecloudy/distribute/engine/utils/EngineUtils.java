package com.maplecloudy.distribute.engine.utils;

import java.io.IOException;
import java.net.ServerSocket;

public class EngineUtils {
  
  public static int getRandomPort() throws IOException
  {
    ServerSocket serverSocket =  new ServerSocket(0); //读取空闲的可用端口
    int port = serverSocket.getLocalPort();
    serverSocket.close();
    return port;
  }
  
}
