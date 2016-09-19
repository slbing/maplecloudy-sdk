package com.maplecloudy.indexer.writer;

import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.solr.common.SolrInputDocument;

import com.maplecloudy.indexer.TopIndexer;
import com.maplecloudy.indexer.hadoop.HadoopIndexer;

public class GeneralWriter<T> extends SolrWriter<T> implements IndexWriter<T> {
  @SuppressWarnings("rawtypes")
  TopIndexer indexer;
  
  public void open(TaskAttemptContext job, Path out) throws Exception {
    super.open(job, out);
    indexer = HadoopIndexer.getIndexerClass(job.getConfiguration());
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public List<SolrInputDocument> createDocment(T data) {
    return indexer.createDocment(data);
  }

	@Override
	public void open(TaskAttemptContext job, Path out, String local,String startShard,String endShard)
			throws Exception {
		open(job,out);
	}

	@Override
	public void open(TaskAttemptContext job, Path out, String local)
			throws Exception {
		open(job,out);
	}
  
}
