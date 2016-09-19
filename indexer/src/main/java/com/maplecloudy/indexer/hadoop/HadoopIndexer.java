package com.maplecloudy.indexer.hadoop;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.maplecloudy.avro.mapreduce.AvroJob;
import com.maplecloudy.indexer.TopIndexer;
import com.maplecloudy.indexer.hadoop.output.IndexerOutputFormat;
import com.maplecloudy.indexer.writer.SolrWriter;
import com.maplecloudy.oozie.main.OozieMain;
import com.maplecloudy.oozie.main.OozieToolRunner;

public class HadoopIndexer extends OozieMain implements Tool {
  public static final Log LOG = LogFactory.getLog(HadoopIndexer.class);
  public static final String TOP_INDEX_CLASS = "top.index.class";
  
  public static class IMapper<K,V,KO,VO> extends Mapper<K,V,KO,VO> {
    TopIndexer<K,V,KO,VO> indexer;
    
    @SuppressWarnings("unchecked")
    protected void setup(Context context) throws IOException,
        InterruptedException {
      indexer = getIndexerClass(context.getConfiguration());
    }
    
    @Override
    protected void map(K k, V v, Context context) throws IOException,
        InterruptedException {
      if (indexer.getData(k, v,context) == null) return;
      context.write(indexer.getIndexKey(k, v), indexer.getData(k, v,context));
    }
  }
  
  public static void setIndexerClass(Job job,
      @SuppressWarnings("rawtypes") Class<? extends TopIndexer> clazz) {
    job.getConfiguration().setClass(TOP_INDEX_CLASS, clazz, TopIndexer.class);
  }
  
  @SuppressWarnings("rawtypes")
  public static TopIndexer getIndexerClass(Configuration job) {
    
    try {
      return job.getClass(TOP_INDEX_CLASS, null, TopIndexer.class)
          .newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
  
  public int index(List<Path> inputs, Path output, String coreName,
      int indexNum,
      @SuppressWarnings("rawtypes") Class<? extends TopIndexer> clazz)
      throws Exception {
    AvroJob job = AvroJob.getAvroJob(getConf());
    job.setJobName("Indexer:" + coreName);
    LOG.info("Indexer starting:" + coreName);
    
    for (Path input : inputs) {
      FileInputFormat.addInputPath(job, input);
    }
    // job.setInputFormatClass(AvroPairInputFormat.class);
    clazz.newInstance().init(job);
    // job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(IMapper.class);
    if ("local".equals(job.getConfiguration().get("mapred.job.tracker"))) {
      // override
      job.setNumReduceTasks(1);
      LOG.info("Indexer: jobtracker is 'local', Indexer exactly one partition.");
    } else job.setNumReduceTasks(indexNum);
    FileOutputFormat.setOutputPath(job, output);
    job.setOutputFormatClass(IndexerOutputFormat.class);
    HadoopIndexer.setIndexerClass(job, clazz);
    job.getConfiguration().set(SolrWriter.SOLR_CORE_NAME, coreName);
    /*
     * job.setOutputKeyClass(Float.class);
     * 
     * job.setOutputValueClass(SpiderData.class);
     */
    try {
      job.waitForCompletion(true);
      LOG.info("Indexer done:" + coreName);
      return 0;
    } catch (IOException e) {
      e.printStackTrace();
      return -1;
    }
  }
  
  public static void main(String[] args) throws Exception {
    int res = OozieToolRunner.run(new Configuration(), new HadoopIndexer(),
        args);
    System.exit(res);
  }
  
  @Override
  public int run(String[] args) throws Exception {
//    return index(
//        Collections.singletonList(new Path("E:/user/sunflower/online/vc_log/extract/DataAnalysisInfo/2013-07-02/00/current")),
//        new Path("E:/index"), "core-da", 1, DataAnalysisInfoIndexer.class);
	  return 0;
  }
}
