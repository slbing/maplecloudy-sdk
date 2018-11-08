package com.maplecloudy.oozie.main;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

import com.maplecloudy.avro.mapreduce.AvroJob;

public abstract class OozieMain extends Configured implements Tool {
  public static final Log LOG = LogFactory.getLog(OozieMain.class);
  public static final String OOZIE_JAVA_MAIN_CAPTURE_OUTPUT_FILE = "oozie.action.output.properties";
  static Properties cp = new Properties();
  
  public void putActionData(String key, String value) {
    cp.put(key, value);
  }
  
  public void setActionDatas(HashMap<String,String> hm) {
    cp.putAll(hm);
  }
  
  @Override
  public int run(String[] args) throws Exception {
    loadOozieConf();
    return 0;
  }
  
  public void loadOozieConf() throws Exception {
    if (System.getProperty("oozie.action.conf.xml") != null)
      getConf().addResource(
          new Path("file:///", System.getProperty("oozie.action.conf.xml")));
  }
  
  public void loadSparkConf() throws Exception {
    Properties props = new Properties();
    props.load(OozieMain.class.getResourceAsStream("/spark-defaults.conf"));
    System.setProperties(props);
  }
  
  public boolean runJob(AvroJob job)
      throws IOException, InterruptedException, ClassNotFoundException {
    job.submit();
    String idf = System.getProperty("oozie.action.newId.properties");
    if (idf != null) {
      
      File idFile = new File(idf);
      String jobId = job.getJobID().toString();
      LOG.info("Save the Job ID:" + jobId + " to the file:" + idf);
      Properties props = new Properties();
      props.setProperty("id", jobId);
      OutputStream os = new FileOutputStream(idFile);
      props.store(os, "");
      os.close();
    }
    return job.waitForCompletion(true);
  }
  
  public void storeData() {
    try {
      if (System.getProperty(OOZIE_JAVA_MAIN_CAPTURE_OUTPUT_FILE) != null) {
        FileOutputStream out = new FileOutputStream(
            System.getProperty(OOZIE_JAVA_MAIN_CAPTURE_OUTPUT_FILE));
        cp.store(out, "");
        out.close();
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    
  }
}
