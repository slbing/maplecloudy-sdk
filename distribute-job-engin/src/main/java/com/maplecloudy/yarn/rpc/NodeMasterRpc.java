package com.maplecloudy.yarn.rpc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.maplecloudy.distribute.engine.utils.YarnCompat;

public class NodeMasterRpc {
  
  private final Configuration cfg;
  private final NMTokenCache tokenCache;
  private NMClient client;
  
  public NodeMasterRpc(Configuration cfg, NMTokenCache tokenCache) {
    this.cfg = new YarnConfiguration(cfg);
    this.tokenCache = tokenCache;
  }
  
  public void start() {
    if (client != null) {
      return;
    }
    
    client = NMClient.createNMClient("Elasticsearch-YARN");
    YarnCompat.setNMTokenCache(client, tokenCache);
    client.init(cfg);
    client.start();
  }
  
  public Map<String,ByteBuffer> startContainer(Container container,
      ContainerLaunchContext containerLaunchContext) throws YarnException, IOException {
    return client.startContainer(container, containerLaunchContext);
  }
  
  public ContainerStatus getContainerState(ContainerId containerId,
      NodeId nodeId) throws YarnException, IOException {
    return client.getContainerStatus(containerId, nodeId);
  }
  
  public void close() throws Exception {
    if (client == null) {
      return;
    }
    
    client.stop();
    client = null;
  }
}
