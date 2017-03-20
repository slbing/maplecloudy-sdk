package com.maplecloudy.indexer;

import java.util.List;

import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.solr.common.SolrInputDocument;

import com.maplecloudy.avro.mapreduce.AvroJob;

/**
 * @author libing.sun
 * 
 * @param <INPUTKEY>
 * @param <INPUTVALUE>
 * @param <INDEXENTITY>
 */

public abstract class TopIndexer<INPUTKEY,INPUTVALUE,INDEXKEY,INDEXENTITY> extends HadoopIndexerBase{
  
  public abstract List<SolrInputDocument> createDocment(INDEXENTITY data);
  
  public abstract INDEXKEY getIndexKey(INPUTKEY k, INPUTVALUE v);
  
  public abstract INDEXENTITY getData(INPUTKEY k, INPUTVALUE v,Context context);
  
  public abstract void init(AvroJob job);
}
