package com.maplecloudy.index.client;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.common.SolrInputDocument;

import com.google.common.collect.Lists;
import com.maplecloudy.indexer.HadoopIndexerBase;

public abstract class SolrClient<T> extends HadoopIndexerBase {
	public static final Log LOG = LogFactory.getLog(SolrClient.class);
	public SolrServer solrServer;
	List<SolrInputDocument> bdocs = Lists.newArrayList();

	public static final int DEFAULT_PER_COMMITNUM = 100000;

	public static final int DEFAULT_PER_ADDDOCNUM = 10000;

	public static final int DEFAULT_PER_LOGNUM = 50000;

	public static final int DEFAULT_PER_COMMITINTERVAL = 15000;

	/**
	 * per num to output log by solr info, the info is to watch the status of
	 * solr to make us to know how solr works
	 */
	private int PER_LOGNUM = DEFAULT_PER_LOGNUM;
	/**
	 * the time between last commit time and now commit time if last time +
	 * PER_COMMIT_INTERVAL > now ,solr commit
	 */
	private int PER_COMMIT_INTERVAL = DEFAULT_PER_COMMITINTERVAL;

	/**
	 * per commit
	 */
	public int PER_COMMITNUM = DEFAULT_PER_COMMITNUM;

	/**
	 * per add
	 */
	private int PER_ADDDOCNUM = DEFAULT_PER_ADDDOCNUM;

	/**
	 * all count
	 */
	int count = 0;
	/**
	 * max add time
	 */
	long maxAddTime = 0;
	/**
	 * null dai count
	 */
	int dainull = 0;
	/**
	 * last commit time
	 */
	long lastCommitTime;

	long averTime = 0;

	int commitfirst = 0;

	public abstract void init(Map<String, String> map) throws Exception;

	public void createIndex(T t) throws Exception {
		List<SolrInputDocument> docs = createDocment(t);
		if (docs == null)
			return;

		bdocs.addAll(docs);
		count += docs.size();
		// wirte first commit time first add doc = first commit time
		if (commitfirst == 0) {
			// first time to add docs 提交
			lastCommitTime = System.currentTimeMillis();
			commitfirst = 1;
		}

		// outtime commit
		if (lastCommitTime + PER_COMMIT_INTERVAL < System.currentTimeMillis()) {
			LOG.info("create outTime commit....... commit time >  "
					+ PER_COMMIT_INTERVAL);
			LOG.info("this time doc num is  " + bdocs.size());
			LOG.info("already commit  doc num is  " + count);
			long s3 = System.currentTimeMillis();
			solrServer.add(bdocs);
			solrServer.commit();
			long s4 = System.currentTimeMillis();
			LOG.info("this time commit cost time is" + (s4 - s3));
			lastCommitTime = System.currentTimeMillis();
			bdocs.clear();
			return;
		}
		if (docs != null && bdocs.size() > 0
				&& bdocs.size() % PER_ADDDOCNUM == 0) {
			long s1 = System.currentTimeMillis();
			solrServer.add(bdocs);
			bdocs.clear();
			long s2 = System.currentTimeMillis();
			long re = s2 - s1;
			if (re > maxAddTime) {
				maxAddTime = re;
				LOG.info("this time create new maxAddTime:" + re);
			}

			if (averTime != 0) {
				averTime = (re + averTime) / 2;
			} else {
				averTime = re;
			}
		}

		// write log
		if (count % PER_LOGNUM == 0) {
			LOG.info("already add doc num:" + count);
			LOG.info("per " + PER_ADDDOCNUM + " add's maxAddTime:" + maxAddTime);
			LOG.info("all avg add " + PER_ADDDOCNUM + " doc 's  avg time is :"
					+ averTime);
		}
		// commit
		if (count % PER_COMMITNUM == 0) {
			long s3 = System.currentTimeMillis();
			if (CollectionUtils.isNotEmpty(bdocs)) {
				solrServer.add(bdocs);
			}
			solrServer.commit();
			long s4 = System.currentTimeMillis();
			lastCommitTime = s4;
			bdocs.clear();
			LOG.info("per " + PER_COMMITNUM + " commit time is : " + (s4 - s3));
		}
	}

	public void deleteIndex(String query) {
		if (solrServer == null) {
			LOG.error("please init server first............. ");
		}

		if (query == null) {
			LOG.error("query is null....................");
		}
		try {
			solrServer.deleteByQuery(query);
		} catch (Exception e) {
			LOG.error("delete index query is: " + query + "............");
			e.printStackTrace();
		}
	}

	public void close() {
		try {
			if (solrServer != null) {

				// commit last thing
				if (bdocs.size() > 0) {
					solrServer.add(bdocs);
					solrServer.commit();
					bdocs.clear();
				}

				// cloudserver.optimize();
				solrServer.shutdown();
				solrServer = null;
				LOG.info("shut down solrserver.......");
			}
		} catch (Exception e) {
			e.printStackTrace();
			e.getMessage();
		}
	}

	public abstract List<SolrInputDocument> createDocment(T t) throws Exception;
}
