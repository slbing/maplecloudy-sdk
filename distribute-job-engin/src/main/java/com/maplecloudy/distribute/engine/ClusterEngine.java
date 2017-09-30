package com.maplecloudy.distribute.engine;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.maplecloudy.distribute.engine.app.elasticsearch.ElatisticSearchPara;

public class ClusterEngine implements AutoCloseable {
  
  private static final Log log = LogFactory.getLog(ClusterEngine.class);
  
  private ApplicationAttemptId appId;
  private final Map<String,String> env;
  private AppMasterRpc rpc;
  private final Configuration cfg;
  private EsCluster cluster;
  private NMTokenCache nmTokenCache;
  private ByteBuffer allTokens;
  private UserGroupInformation appSubmitterUgi;
  private RegisterApplicationMasterResponse amResponse;
  
  private final ElatisticSearchPara para;
  
  ClusterEngine(Map<String,String> env) {
    this.env = env;
    cfg = new YarnConfiguration();
//    if (env.containsKey(FS_URI)) {
//      cfg.set(FileSystem.FS_DEFAULT_NAME_KEY, env.get(FS_URI));
//    }
    para = ElatisticSearchPara.getFromCfg(cfg);
  }
  
  void run() throws IOException, YarnException {
    log.info("Starting ApplicationMaster...");
    
    if (nmTokenCache == null) {
      
      nmTokenCache = new NMTokenCache();
    }
    // Note: Credentials, Token, UserGroupInformation, DataOutputBuffer class
    // are marked as LimitedPrivate
    // Credentials credentials = UserGroupInformation.getCurrentUser()
    // .getCredentials();
    // DataOutputBuffer dob = new DataOutputBuffer();
    // credentials.writeTokenStorageToStream(dob);
    // // Now remove the AM->RM token so that containers cannot access it.
    // Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
    //
    // System.out.println("Executing with tokens:");
    // while (iter.hasNext()) {
    // Token<?> token = iter.next();
    // System.out.println(token);
    // if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
    // iter.remove();
    // }
    // }
    // allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    //
    // // Create appSubmitterUgi and add original tokens to it
    // String appSubmitterUserName = System
    // .getenv(ApplicationConstants.Environment.USER.name());
    // appSubmitterUgi = UserGroupInformation
    // .createRemoteUser(appSubmitterUserName);
    // appSubmitterUgi.addCredentials(credentials);
    // System.out.println("appSubmitterUgi-----------"
    // + appSubmitterUgi.toString());
    
    if (rpc == null) {
      rpc = new AppMasterRpc(cfg, nmTokenCache);
      rpc.start();
    }
    
    // register AM
//    appId = YarnUtils.getApplicationAttemptId(env);
//    Assert.notNull(appId, "ApplicationAttemptId cannot be found in env %s"
//        + env);
    amResponse = rpc.registerAM();
    
    updateTokens();
    
    cluster = new EsCluster(rpc, para, env);
    try {
      cluster.start();
    } finally {
      try {
        close();
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
  
  private void updateTokens() {
    for (NMToken nmToken : amResponse.getNMTokensFromPreviousAttempts()) {
      nmTokenCache.setToken(nmToken.getNodeId().toString(), nmToken.getToken());
    }
  }
  
  public void close() throws Exception {
    boolean hasFailed = (cluster == null || cluster.hasFailed());
    
    try {
      if (cluster != null) {
        cluster.close();
        cluster = null;
      }
      
    } finally {
      if (amResponse != null) {
        updateTokens();
      }
      if (hasFailed) {
        rpc.failAM();
      } else {
        rpc.finishAM();
      }
    }
  }
  
  public static void main(String[] args) throws Exception {
 
    ClusterEngine am = new ClusterEngine(System.getenv());
    try {
      am.run();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      
      am.close();
    }
  }
}