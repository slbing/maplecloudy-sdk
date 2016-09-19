package com.maplecloudy.indexer.writer;

import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.SolrCore;

import com.google.common.collect.Lists;
import com.maplecloudy.indexer.solr.CustomCoreContainer;


public abstract class SolrWriter<T> implements IndexWriter<T> {
 public static final Log LOG = LogFactory.getLog(SolrWriter.class);
  public static String SOLR_CORE_NAME = "solr.core.name";
  public static String SOLR_WRITER_NAME = "solr.writer.name";
  public static final String DONE_NAME = "index.done";
  
  CustomCoreContainer container;
  EmbeddedSolrServer server;
  
  private Path perm;
  
  private Path temp;
  
  private FileSystem fs;
  
  public abstract List<SolrInputDocument> createDocment(T data);
  
  public void open(TaskAttemptContext job, Path out) throws Exception {
    this.fs = FileSystem.get(job.getConfiguration());
    perm = out;
    temp = job.getConfiguration().getLocalPath("mapred.local.dir",
        "_" + Integer.toString(new Random().nextInt()));
    String coreName = job.getConfiguration().get(SOLR_CORE_NAME);
    container = new CustomCoreContainer(coreName, temp.toString());
    server = new EmbeddedSolrServer(container, coreName);
    fs.delete(perm, true);
  }
  
  public void close() throws Exception {
    if (bdocs.size() > 0  ) {
      server.add(bdocs);
      server.commit();
      server.optimize();
      bdocs.clear();
    }
    LOG.info("last info:..............."  );
    LOG.info("last info:..............."  );
    LOG.info("last info:..............."  );
    LOG.info("full num:" + count );
    
//    long avtime = 0;
//	for (long time : averageTime) {
//		avtime += time;
//	}
//	avtime = avtime / averageTime.size();
//	LOG.info("all avg add 10000 doc 's  avg time is :" + avtime);
    
    for (SolrCore core : container.getCores()) {
      core.close();
    }
    server.shutdown();
    fs.completeLocalOutput(perm, temp); // copy to dfs
    fs.createNewFile(new Path(perm, DONE_NAME));
  }
  List<SolrInputDocument> bdocs = Lists.newArrayList();
  int count = 0;
  
  long maxAddTime = 0;
  int dainull = 0;
  int peraddconut = 10000;
  
//  List<Long> averageTime = Lists.newArrayList();
  @Override
  public void write(T data) throws Exception {
	  List<SolrInputDocument> docs = createDocment((T) data);
	    bdocs.addAll(docs);
	    count += docs.size();
	    if (docs != null && bdocs.size() > 0 && bdocs.size() % 10000 == 0) {
	      long s1 = System.currentTimeMillis();
	      server.add(bdocs);
	      long s2 = System.currentTimeMillis();
	      long re = s2 - s1;
	      if(re > maxAddTime){
	    	  maxAddTime = re;
	    	  LOG.info("this time create new maxAddTime:" + re);
	      }
//	      averageTime.add(re);
	      
	      bdocs.clear();
	      // for (SolrInputDocument doc : docs)
	      // server.add(doc);
	    }
	    if(count % 50000 == 0){
	    	LOG.info("already add doc num:" + count );
	    	LOG.info("per 10000 add's maxAddTime:" + maxAddTime );
//	    	long avtime = 0;
//	    	for (long time : averageTime) {
//	    		avtime += time;
//			}
//	    	avtime = avtime / averageTime.size();
//	    	LOG.info("all avg add 10000 doc 's  avg time is :" + avtime);
	    }
	    if(count % 100000 == 0){
	    	long s1 = System.currentTimeMillis();
	    	server.commit();
	    	long s2 = System.currentTimeMillis();
	    	LOG.info("per 100000 commit time is : " + (s2 - s1));
	    }
	  }
}
