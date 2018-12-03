/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.maplecloudy.yarn.rpc;

import java.util.EnumSet;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.google.common.collect.Maps;

public class ClientRpc {
  

  private static final EnumSet<YarnApplicationState> ALIVE = EnumSet.range(
      YarnApplicationState.NEW, YarnApplicationState.RUNNING);
  
  private HashMap<String,YarnClient> clients = Maps.newHashMap();
  
  private ClientRpc() {
    
  }
  
  static ClientRpc yarnClients = new ClientRpc();
  
  static ClientRpc getInstance() {
    return yarnClients;
  }
  
  public static YarnClient getYarnClient(Configuration conf) {
    String resourceManagerAddress = conf.get("yarn.resourcemanager.address");
    String defaultFS = conf.get("fs.defaultFS");
    YarnClient client = ClientRpc.getInstance().clients
        .get(resourceManagerAddress);
    if (client == null) {
      client = YarnClient.createYarnClient();
      Configuration yconf = new YarnConfiguration(conf);
      client.init(yconf);
      client.start();
      ClientRpc.getInstance().clients.put(resourceManagerAddress, client);
    }
    return client;
  }
  
 
  
//  public void waitForApp(ApplicationId appId, long timeout)
//      throws InterruptedException, YarnException, IOException {
//    boolean repeat = false;
//    long start = System.currentTimeMillis();
//    do {
//      ApplicationReport appReport = client.getApplicationReport(appId);
//      YarnApplicationState appState = appReport.getYarnApplicationState();
//      repeat = (appState != YarnApplicationState.FINISHED
//          && appState != YarnApplicationState.KILLED && appState != YarnApplicationState.FAILED);
//      if (repeat) {
//        Thread.sleep(500);
//      }
//      
//    } while (repeat && (System.currentTimeMillis() - start) < timeout);
//  }
  
  public void close() {
    if (clients != null) {
      for(YarnClient client : clients.values())
      {
        client.stop();
        client = null;
      }
      clients.clear();
    }
  }
  
}