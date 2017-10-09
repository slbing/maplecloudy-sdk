
package com.maplecloudy.distribute.engine.utils;

import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.*;

public abstract class YarnUtils {

    public static ApplicationAttemptId getApplicationAttemptId(Map<String, String> env) {
        if (env == null) {
            return null;
        }
        String amContainerId = env.get(ApplicationConstants.Environment.CONTAINER_ID.name());
        if (amContainerId == null) {
            return null;
        }
        ContainerId containerId = ConverterUtils.toContainerId(amContainerId);
        return containerId.getApplicationAttemptId();
    }

    public static InetSocketAddress getResourceManagerAddr(Configuration cfg) {
        return cfg.getSocketAddr(RM_SCHEDULER_ADDRESS, DEFAULT_RM_SCHEDULER_ADDRESS, DEFAULT_RM_SCHEDULER_PORT);
    }

    public static long getAmHeartBeatRate(Configuration cfg) {
        return cfg.getLong(RM_AM_EXPIRY_INTERVAL_MS, DEFAULT_RM_AM_EXPIRY_INTERVAL_MS);
    }

    
    public static Map<String,String>  setupAppMasterEnv(Configuration conf) {
      Map<String, String> appMasterEnv = new LinkedHashMap<String, String>(); 
      
      StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$())
          .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
      for (String c : conf.getStrings(
          YarnConfiguration.YARN_APPLICATION_CLASSPATH,
          YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
        classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
        classPathEnv.append(c.trim());
      }
      appMasterEnv.put(Environment.CLASSPATH.name(), classPathEnv.toString());
      
      return appMasterEnv;
    }
    
    public static Map<String, String> setupEnv(Configuration cfg) {
      Map<String, String> env = new LinkedHashMap<String, String>(); // System.getenv()
      // add Hadoop Classpath
      for (String c : cfg.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnCompat.DEFAULT_PLATFORM_APPLICATION_CLASSPATH())) {
          addToEnv(env, Environment.CLASSPATH.name(), c.trim());
      }
      // add es-hadoop jar / current folder jars
      addToEnv(env, Environment.CLASSPATH.name(), "./*");

      //
      // some es-yarn constants
      //elasticsearch-5.3.0.zip/elasticsearch-5.3.0/bin/elasticsearch 
//      addToEnv(env, EsYarnConstants.FS_URI, cfg.get(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS));

      return env;
  }
    
    public static void addToEnv(Map<String, String> env, String key, String value) {
        String val = env.get(key);
        if (val == null) {
            val = value;
        }
        else {
            val = val + YarnCompat.CLASS_PATH_SEPARATOR() + value;
        }
        env.put(key, val);
    }

    public static void addToEnv(Map<String, String> env, Map<String, String> envVars) {
        for (Entry<String, String> entry : envVars.entrySet()) {
            addToEnv(env, entry.getKey(), entry.getValue());
        }
    }

    public static Object minVCores(Configuration cfg, int vCores) {
        return yarnAcceptableMin(cfg, RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, vCores);
        //return vCores;
    }

    public static int minMemory(Configuration cfg, int memory) {
        return yarnAcceptableMin(cfg, RM_SCHEDULER_MINIMUM_ALLOCATION_MB, DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB, memory);
        //return memory;
    }

    private static int yarnAcceptableMin(Configuration cfg, String property, int defaultValue, int value) {
        int acceptedVal = cfg.getInt(property, defaultValue);
        if (acceptedVal <= 0) {
            acceptedVal = defaultValue;
        }
        if (acceptedVal >= value) {
            return acceptedVal;
        }
        if (value % acceptedVal != 0) {
            return acceptedVal * Math.round(value / acceptedVal);
        }
        return value;
    }

    public static ApplicationId createAppIdFrom(String appId) {
        appId = appId.substring(ApplicationId.appIdStrPrefix.length());
        int delimiter = appId.indexOf("-");
        return ApplicationId.newInstance(Long.parseLong(appId.substring(0, delimiter)), Integer.parseInt(appId.substring(delimiter + 1)));
    }
}