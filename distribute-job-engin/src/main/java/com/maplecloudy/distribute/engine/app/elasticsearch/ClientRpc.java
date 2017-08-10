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
package com.maplecloudy.distribute.engine.app.elasticsearch;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

public class ClientRpc implements AutoCloseable {
  
  private static final Set<String> ES_TYPE = Collections
      .singleton("ELASTICSEARCH");
  private static final EnumSet<YarnApplicationState> ALIVE = EnumSet.range(
      YarnApplicationState.NEW, YarnApplicationState.RUNNING);
  
  private YarnClient client;
  private final Configuration cfg;
  
  public ClientRpc(Configuration cfg) {
    this.cfg = new YarnConfiguration(cfg);
  }
  
  public void start() {
    if (client != null) {
      return;
    }
    
    UserGroupInformation.setConfiguration(cfg);
    
    client = YarnClient.createYarnClient();
    client.init(cfg);
    client.start();
  }
  
  public YarnClientApplication newApp() throws YarnException, IOException {
    return client.createApplication();
  }
  
  public ApplicationId submitApp(ApplicationSubmissionContext appContext)
      throws YarnException, IOException {
    return client.submitApplication(appContext);
    
  }
  
  public List<ApplicationReport> killEsApps() throws YarnException, IOException {
    List<ApplicationReport> esApps = client.getApplications(ES_TYPE, ALIVE);
    
    for (ApplicationReport appReport : esApps) {
      client.killApplication(appReport.getApplicationId());
    }
    
    return esApps;
    
  }
  
  public ApplicationReport getReport(ApplicationId appId) throws YarnException,
      IOException {
    return client.getApplicationReport(appId);
  }
  
  public List<ApplicationReport> listApps() throws YarnException, IOException {
    return client.getApplications();
  }
  
  public List<ApplicationReport> listEsClusters() throws YarnException,
      IOException {
    return client.getApplications(ES_TYPE);
  }
  
  public List<ApplicationReport> listEsClustersAlive() throws YarnException,
      IOException {
    return client.getApplications(ES_TYPE, ALIVE);
  }
  
  public void waitForApp(ApplicationId appId, long timeout)
      throws InterruptedException, YarnException, IOException {
    boolean repeat = false;
    long start = System.currentTimeMillis();
    do {
      ApplicationReport appReport = client.getApplicationReport(appId);
      YarnApplicationState appState = appReport.getYarnApplicationState();
      repeat = (appState != YarnApplicationState.FINISHED
          && appState != YarnApplicationState.KILLED && appState != YarnApplicationState.FAILED);
      if (repeat) {
        Thread.sleep(500);
      }
      
    } while (repeat && (System.currentTimeMillis() - start) < timeout);
  }
  
  public void close() {
    if (client != null) {
      client.stop();
      client = null;
    }
  }
  
  public Configuration getConfiguration() {
    return cfg;
  }
}