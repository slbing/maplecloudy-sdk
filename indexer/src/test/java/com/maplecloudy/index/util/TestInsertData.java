package com.maplecloudy.index.util;

import java.util.List;

import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestInsertData {
//	@Test
	public void testInsert() throws Exception{
		CloudSolrServer cloudserver = new CloudSolrServer("127.0.0.1:9983");
		cloudserver.setDefaultCollection("collection2");
		List<SolrInputDocument> lst = Lists.newArrayList();
		for (int i = 0; i < 100; i++) {
			SolrInputDocument doc = new SolrInputDocument();
			doc.addField("sid", "87654321");
			doc.addField("age_s", "cc"  + i );
			doc.addField("_shard_", "shard1");
			lst.add(doc);
		}
		cloudserver.add(lst);
		cloudserver.commit();
		cloudserver.shutdown();
	}
	
//	@Test
	public void testOptimize() throws Exception{
//		CloudSolrServer cloudserver = new CloudSolrServer("127.0.0.1:9983");
//		cloudserver.setDefaultCollection("collection2");
//		List<SolrInputDocument> lst = Lists.newArrayList();
//		for (int i = 0; i < 100; i++) {
//			SolrInputDocument doc = new SolrInputDocument();
//			doc.addField("sid", "87654321");
//			doc.addField("age_s", "cc"  + i );
//			doc.addField("_shard_", "shard2");
//			lst.add(doc);
//		}
//		cloudserver.add(lst);
//		cloudserver.commit();
//		cloudserver.shutdown();
		
		HttpSolrServer solr = new HttpSolrServer("http://172.16.3.174:8983/solr/collection2");
		solr.optimize();
		solr.shutdown();
	}
}
