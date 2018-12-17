package com.maplecloudy.spider.protocol.httpmethod;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
//import org.apache.http.client.config.CookieSpecs;
//import org.apache.http.client.config.RequestConfig;
//import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.impl.client.DefaultHttpClient;
//import org.apache.http.impl.client.CloseableHttpClient;
//import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.tophits.ParsedTopHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.jsoup.Connection;
import org.jsoup.Jsoup;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.maplecloudy.spider.protocol.Content;

public class ProxyWithEs {

	private static String ES_IP = "localhost";
	private static int ES_PORT = 9200;
	private static String ES_INDEX = "proxy";
	private static String ES_TYPE = "_doc";

	private static int PROXY_SIZE = 1000;
	private static String SCORE = "score";

	private final static Object OBJECT = new Object();

	private static ProxyWithEs proxyWithEs;
	private static RestHighLevelClient client;

	private static List<String> proxyList = Lists.newArrayList();
	private static Map<String, Integer> proxyScore = Maps.newHashMap();
	private static Random random = new Random();

	private ProxyWithEs() {
	}

	private ProxyWithEs(Configuration conf) {
	}

	public static ProxyWithEs getInstance() {
		if (proxyWithEs != null)
			return proxyWithEs;
		synchronized (OBJECT) {
			if (proxyWithEs == null) {
				proxyWithEs = new ProxyWithEs();
			}
		}
		return proxyWithEs;
	}

	public void setUp() {
		client = new RestHighLevelClient(
				RestClient.builder(new HttpHost(ES_IP, ES_PORT, "http")));
		SearchRequest request = new SearchRequest(ES_INDEX);
		SearchSourceBuilder sBuilder = new SearchSourceBuilder();
		sBuilder.size(PROXY_SIZE);
		sBuilder.sort(Model.score, SortOrder.ASC);
		try {
			SearchResponse response = client.search(request.source(sBuilder), RequestOptions.DEFAULT);
			SearchHit[] hits = response.getHits().getHits();
			int s = hits.length;
			for (int i = 0; i < s; i++) {
				String ip2port = hits[i].getId();
				proxyList.add(ip2port);
				proxyScore.put(ip2port, 0);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			closeClient();
		}
	}
	
	private void setUpBak() {
		client = new RestHighLevelClient(
				RestClient.builder(new HttpHost(ES_IP, ES_PORT, "http")));
		SearchRequest request = new SearchRequest(ES_INDEX);
		SearchSourceBuilder sBuilder = new SearchSourceBuilder();
		sBuilder.size(1);
		AggregationBuilder aggBuilder = AggregationBuilders
				.terms(Model.ip)
				.field(Model.ip + ".keyword")
				.size(PROXY_SIZE)
				.order(BucketOrder.aggregation(Model.score, true))
				.subAggregation(
						AggregationBuilders.sum(Model.score).field(Model.score)
				)
				.subAggregation(
						AggregationBuilders.topHits(Model.class.getSimpleName()).size(1)
				);
		
		sBuilder.aggregation(aggBuilder);
		try {
			SearchResponse response = client.search(request.source(sBuilder), RequestOptions.DEFAULT);
			System.out.println(response);
			ParsedStringTerms agg =  (ParsedStringTerms) response.getAggregations().getAsMap().get(Model.ip);
			List<? extends Terms.Bucket> termBuckets = agg.getBuckets();
			for (Bucket bucket : termBuckets) {
				ParsedTopHits topHits = bucket.getAggregations().get(Model.class.getSimpleName());
				Map<String, Object> model = topHits.getHits().getAt(0).getSourceAsMap();
				System.out.println(model);
				String ip2port = (String) model.get(Model.ip) + model.get(Model.port);
				proxyList.add(ip2port);
				proxyScore.put(ip2port, 0);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			closeClient();
		}
	}
	
	public void closeDown() {
		client = new RestHighLevelClient(
				RestClient.builder(new HttpHost(ES_IP, ES_PORT, "http")));
		BulkRequest request = new BulkRequest();
		for (Entry<String, Integer> ip2port: proxyScore.entrySet()) {
			Map<String, Object> parameters = Maps.newHashMap();
			parameters.put(Model.score, ip2port.getValue());
			Script inline = new Script(ScriptType.INLINE, "painless", "ctx._source."+ Model.score +" += params." + Model.score, parameters);  
			request.add(new UpdateRequest(ES_INDEX, ES_TYPE, ip2port.getKey()).script(inline));
		}
		try {
			BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			closeClient();
		}
	}
	
	/*
	 *  @param ip2port : 最大为1000；内容为：127.0.0.1:9200
	 */
	public boolean proxyToEs(List<String> ip2portList) {
		if (ip2portList.size() > 1000) {
			System.out.println(" proxy size > 1000 ; please control size in 1000");
			return false;
		}
		client = new RestHighLevelClient(
				RestClient.builder(new HttpHost(ES_IP, ES_PORT, "http")));
		BulkRequest request = new BulkRequest();
		for (String ip2port : ip2portList) {
			String[] ipAndPort = ip2port.split(":");
			request.add(new IndexRequest(ES_INDEX, ES_TYPE, ip2port)
					.source(String.format(Model.proxyModel, ipAndPort[0], Integer.valueOf(ipAndPort[1])), XContentType.JSON));
		}
		try {
			client.bulk(request, RequestOptions.DEFAULT);
//			BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			closeClient();
		}
		return true;
	}

	private void closeClient() {
		try {
			client.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public synchronized void setScore(String ip2port) {
		proxyScore.put(ip2port, proxyScore.get(ip2port) + 1);
	}
	
	public synchronized String getProxy() {
		int size = proxyList.size();
		if (size == 0) return null;
		return proxyList.get(random.nextInt(size));
	}

	
	private interface Model {
		String ip = "ip";
		String port = "port";
		String score = "score";
		String time = "time";
		String proxyModel = "{" +
		        "\""+ ip +"\":\"%s\"," +
		        "\""+ port +"\":\"%s\"," +
		        "\""+ score +"\":0," +
		        "\""+ time +"\":\"" + new Date().toString() + "\"" +
		        "}";
	}
	private final static int TIME_OUT = 5 * 1000;
	private final static int MAX_SIZE = 10 * 1024 * 1024;
//	private static CloseableHttpClient httpClient = HttpClients.createDefault();
//	private static RequestConfig.Builder builder = RequestConfig.custom().setSocketTimeout(TIME_OUT).
//            setConnectTimeout(TIME_OUT).
//            setConnectionRequestTimeout(TIME_OUT).
//            setCookieSpec(CookieSpecs.IGNORE_COOKIES);

	public static void main(String[] args) throws IOException, InterruptedException {
//		List<String> s = Arrays.asList("47.52.153.167:80", "47.52.238.112:8118", "47.75.194.109:80", "47.88.192.22:8080",
//    			"47.52.24.132:8118", "47.75.8.192:80", "47.52.210.47:80", "47.75.48.149:80", "47.52.208.159:80", "47.52.153.167:443", "47.52.209.8:80",
//    			"47.75.64.102:80", "47.75.62.90:80", "47.91.237.251:80", "47.52.155.245:80", "47.52.64.149:80");
//    	ProxyWithEs.getInstance().proxyToEs(s);
		List<String> e = Lists.newArrayList();
		String eString = Jsoup.connect("http://maplecloudy.v4.dailiyun.com/query.txt?key=NPEE573347&word=&count=1000&rand=true&detail=true").ignoreContentType(true).get().toString().trim();
		System.out.println(eString);
		eString = eString.split("<body>")[1].split("</body>")[0].trim();
		String[] se = eString.split(" ");
		for (int i = 0; i < se.length; i++) {
			e.add(se[i].split(",")[0]);
		}
//		ProxyWithEs.getInstance().proxyToEs(e);
//		System.exit(0);
//		ProxyWithEs.getInstance().setUp();
		int i=0;
		String url = "https://news.pconline.com.cn/1211/12117584.html";
		AtomicInteger as = new AtomicInteger(0);
		AtomicInteger bs = new AtomicInteger(0);
		AtomicInteger cs = new AtomicInteger(0);

		HttpClient httpClient = new DefaultHttpClient();
//		List<String> e = ProxyWithEs.getInstance().proxyList;
		System.out.println("***********  " + e.size());

		for (String ew : e) {
			
			new Thread(new Runnable() {
				@Override
				public void run() {
//					
					String ip2port = ew;
					System.out.println(ip2port);
					String[] proxy = ip2port.split(":");
					HttpGet http = new HttpGet(url);
//				String ip2port = ProxyWithEs.getInstance().getProxy();
					http.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, new HttpHost(proxy[0], Integer.valueOf(proxy[1])));
//					http.setConfig(builder.setProxy(new HttpHost(proxy[0], Integer.valueOf(proxy[1]))).build());
//					http.addHeader("Content-Type", "application/x-www-form-urlencoded");
//					http.addHeader("Cookie", "TC-V5-G0=634dc3e071d0bfd86d751caf174d764e; SUB=_2AkMstywdf8NxqwJRmP4WzW3rZYl1wg3EieKa693GJRMxHRl-yj9jqnM6tRB6BzcC8raDxDR91reemrTMmH_aeOB9hwg6; SUBP=0033WrSXqPxfM72-Ws9jqgMF55529P9D9W5fD9H6XsZ3wx_HpHLR1.sX");
//					CloseableHttpResponse response = null;
					HttpResponse response = null;
					try {
						response = httpClient.execute(http);		
						System.out.println("+++++++++++++++"+response.getStatusLine().getStatusCode());
						if (response.getStatusLine().getStatusCode() == 200) {
							as.incrementAndGet();
						}
					} catch (Exception e1) {
					} finally {
//						try {
//							if (response != null) {
////								response.close();
//							}
//						} catch (IOException e1) {
//							e1.printStackTrace();
//						}
					}
					try {
						Connection.Response response2 = Jsoup.connect(url).proxy(proxy[0], Integer.valueOf(proxy[1])).timeout(5000).ignoreContentType(true).execute();
						System.out.println("--------------------"+response2.statusCode());
						if (response2.statusCode() == 200) {
							bs.incrementAndGet();
//							ProxyWithEs.getInstance().setScore(ip2port);
//							System.out.println(Jsoup.parse(response2.body()).title());
						}
					} catch (Exception e1) {
//						e1.printStackTrace();
						// TODO: handle exception
					}	
					cs.incrementAndGet();
					System.out.println( " yyyyyyyyyyyy  as " + as.intValue() + " yyyyyyyyyyyy  bs " + bs.intValue());
				}
			}).start();
		}
		while (true) {
			if (cs.intValue() == e.size()) {
//				ProxyWithEs.getInstance().closeDown();
				break;
			}
			System.out.println(" ccccccccccccccc  " + cs.intValue());
			Thread.sleep(1000);
		}
	}
	
}
