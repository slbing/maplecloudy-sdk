package com.maplecloudy.index.client;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.LBHttpSolrServer;
import org.apache.solr.common.params.ModifiableSolrParams;

import com.maplecloudy.maple.util.ReadPropertyUtil;

public abstract class CloudSolrClient<T> extends SolrClient<T> {

	public static final Log LOG = LogFactory.getLog(CloudSolrClient.class);

//	private static boolean init = false;

	private static final String DEFAULT_ZKHOST = "vortex-dev.solr0001.hf.voicecloud.cn:2181,vortex-dev.solr0002.hf.voicecloud.cn:2181,vortex-dev.solr0003.hf.voicecloud.cn:2181";

	private static final String DEFAULT_COLLECTION = "collection1";

	private static final int DEFAULT_ZKCONNECTTIMEOUT = 10000;

	private static final int DEFAULT_ZKCLIENTTIMEOUT = 180000;

	/**
	 * ZKHOST address
	 */
	private static String ZKHOST;
	/**
	 * solr collection
	 */
	private static String COLLECTION;

	/**
	 * zk client connection time out
	 */
	private static int ZKCONNECTTIMEOUT;

	/**
	 * zk client read time out
	 */
	private static int ZKCLIENTTIMEOUT;

	/**
	 * init solr cloud server private to debug myself
	 * 
	 * @return boolean
	 * @throws Exception
	 */
	public synchronized void init() throws Exception {

		ZKHOST = ReadPropertyUtil.getStringValue("solr.properties", "zkHost",
				DEFAULT_ZKHOST);

		COLLECTION = ReadPropertyUtil.getStringValue("solr.properties",
				"defaultCollection", DEFAULT_COLLECTION);

		ZKCONNECTTIMEOUT = ReadPropertyUtil.getIntValue("solr.properties",
				"zkconnecttimeout", DEFAULT_ZKCONNECTTIMEOUT);

		ZKCLIENTTIMEOUT = ReadPropertyUtil.getIntValue("solr.properties",
				"zkclienttimeout", DEFAULT_ZKCLIENTTIMEOUT);

		init(ZKHOST, COLLECTION, ZKCONNECTTIMEOUT, ZKCLIENTTIMEOUT, -1, -1, -1,
				-1);
	}

	/**
	 * 
	 * @param zkHost
	 *            set null make it defaute
	 * @param collection
	 *            set null make it defaute
	 * @param zkconnecttimeout
	 *            set it -1 to set defaute value
	 * @param zkclienttimeout
	 *            set it -1 to set defaute value
	 * @param commitnum
	 *            this num is tell solr server how many doc to commit set it -1
	 *            to set defaute value
	 * @param adddocnum
	 *            this num is tell solr how many doc when solr server add
	 *            doclist once set it -1 to set defaute value
	 * @param lognum
	 *            this num is tell solr when reach this num,solr output log help
	 *            us to know it's performance set it -1 to set defaute value
	 * @throws Exception
	 */
	public synchronized void init(String zkHost, String collection,
			int zkconnecttimeout, int zkclienttimeout, int commitnum,
			int adddocnum, int lognum, int commitInterval) throws Exception {
		try {
//			if (!init) {
				if (StringUtils.isBlank(zkHost)) {
					zkHost = DEFAULT_ZKHOST;
				}

				if (StringUtils.isBlank(collection)) {
					collection = DEFAULT_COLLECTION;
				}
				if (zkconnecttimeout == -1) {
					zkconnecttimeout = DEFAULT_ZKCONNECTTIMEOUT;
				}

				if (zkclienttimeout == -1) {
					zkclienttimeout = DEFAULT_ZKCLIENTTIMEOUT;
				}
				
				ModifiableSolrParams params = new ModifiableSolrParams();
				params.set(HttpClientUtil.PROP_MAX_CONNECTIONS, 500);//10
				params.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, 20);//5
				HttpClient client = HttpClientUtil.createClient(params);						
				LBHttpSolrServer lbServer = new LBHttpSolrServer(client);
				solrServer = new CloudSolrServer(zkHost, lbServer);
				 
				((CloudSolrServer) solrServer).setDefaultCollection(collection);
				((CloudSolrServer) solrServer)
						.setZkConnectTimeout(zkconnecttimeout);
				((CloudSolrServer) solrServer)
						.setZkClientTimeout(zkclienttimeout);
				
				this.PER_COMMITNUM = commitnum;
				LOG.info("初始化solr server params:" + "zkHost:" + zkHost
						+ " collection:" + collection + "commitnum :"
						+ this.PER_COMMITNUM);
//				init = true;
//			} else {
//				LOG.error("solr sever aleady init ,please donn't init again");
//			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	@Override
	public void init(Map<String, String> map) throws Exception {
		String zkHost = map.get("zkHost");
		String collection = map.get("collection");
		int zkconnecttimeout = Integer.parseInt(map.get("zkconnecttimeout"));
		int zkclienttimeout = Integer.parseInt(map.get("zkclienttimeout"));
		int commitnum = Integer.parseInt(map.get("commitnum"));
		int adddocnum = Integer.parseInt(map.get("adddocnum"));
		int lognum = Integer.parseInt(map.get("lognum"));
		int commitInterval = Integer.parseInt(map.get("commitInterval"));
		init(zkHost, collection, zkconnecttimeout, zkclienttimeout, commitnum,
				adddocnum, lognum, commitInterval);
	}

}
