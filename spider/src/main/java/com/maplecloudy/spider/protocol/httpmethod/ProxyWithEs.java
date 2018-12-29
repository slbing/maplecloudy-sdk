package com.maplecloudy.spider.protocol.httpmethod;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.http.HttpHost;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.action.bulk.BulkRequest;
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
import org.jsoup.Jsoup;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class ProxyWithEs {
  
  private static String ES_IP = "es1.ali.szol.bds.com";
  private static int ES_PORT = 9200;
  private static String ES_INDEX = "proxy";
  private static String ES_TYPE = "_doc";
  
  private static int PROXY_SIZE = 1000;
  
  private final static Object OBJECT = new Object();
  private final static String IP_URL = "http://maplecloudy.v4.dailiyun.com/query.txt?key=NPEE573347&word=&count=200&rand=true&detail=false";
  
  private static ProxyWithEs proxyWithEs;
  private RestHighLevelClient client;
  
  private static List<String> proxyList = Lists.newArrayList();
  private static Map<String,Integer> proxyScore = Maps.newHashMap();
  private static Random random = new Random();
  
  private static volatile boolean flag = false;
  private static volatile boolean flag2 = true;
  
  private Timer timer;
  
  private ProxyWithEs() {}
  
  private ProxyWithEs(Configuration conf) {}
  
  public static ProxyWithEs getInstance() {
    if (proxyWithEs != null) return proxyWithEs;
    synchronized (OBJECT) {
      if (proxyWithEs == null) {
        proxyWithEs = new ProxyWithEs();
      }
    }
    return proxyWithEs;
  }
  
  public synchronized void setUp() {
    if (flag) return;
    timer = new Timer();
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          String eString = Jsoup.connect(IP_URL).ignoreContentType(true).get()
              .toString().trim();
          eString = eString.split("<body>")[1].split("</body>")[0].trim();
          String[] se = eString.split(" ");
          if (eString.length() > 10) {
            proxyList.clear();
            for (int i = 0; i < se.length; i++) {
              proxyList.add(se[i].split(",")[0]);
            }
            proxyToEs(proxyList);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }, 0, 3 * 60 * 1000);
    flag = true;
  }
  
  public synchronized void close() {
    if (timer != null && flag2) {
      timer.cancel();
      flag2 = false;
    }
  }
  
  public void setUpBak1() {
    client = new RestHighLevelClient(
        RestClient.builder(new HttpHost(ES_IP, ES_PORT, "http")));
    SearchRequest request = new SearchRequest(ES_INDEX);
    SearchSourceBuilder sBuilder = new SearchSourceBuilder();
    sBuilder.size(PROXY_SIZE);
    sBuilder.sort(Model.score, SortOrder.ASC);
    try {
      SearchResponse response = client.search(request.source(sBuilder),
          RequestOptions.DEFAULT);
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
    AggregationBuilder aggBuilder = AggregationBuilders.terms(Model.ip)
        .field(Model.ip + ".keyword").size(PROXY_SIZE)
        .order(BucketOrder.aggregation(Model.score, true))
        .subAggregation(AggregationBuilders.sum(Model.score).field(Model.score))
        .subAggregation(
            AggregationBuilders.topHits(Model.class.getSimpleName()).size(1));
    
    sBuilder.aggregation(aggBuilder);
    try {
      SearchResponse response = client.search(request.source(sBuilder),
          RequestOptions.DEFAULT);
      System.out.println(response);
      ParsedStringTerms agg = (ParsedStringTerms) response.getAggregations()
          .getAsMap().get(Model.ip);
      List<? extends Terms.Bucket> termBuckets = agg.getBuckets();
      for (Bucket bucket : termBuckets) {
        ParsedTopHits topHits = bucket.getAggregations()
            .get(Model.class.getSimpleName());
        Map<String,Object> model = topHits.getHits().getAt(0).getSourceAsMap();
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
    for (Entry<String,Integer> ip2port : proxyScore.entrySet()) {
      Map<String,Object> parameters = Maps.newHashMap();
      parameters.put(Model.score, ip2port.getValue());
      Script inline = new Script(ScriptType.INLINE, "painless",
          "ctx._source." + Model.score + " += params." + Model.score,
          parameters);
      request.add(new UpdateRequest(ES_INDEX, ES_TYPE, ip2port.getKey())
          .script(inline));
    }
    try {
      client.bulk(request, RequestOptions.DEFAULT);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      closeClient();
    }
  }
  
  public void proxyToEs(List<String> ip2portList) {
    client = new RestHighLevelClient(
        RestClient.builder(new HttpHost(ES_IP, ES_PORT, "http")));
    BulkRequest request = new BulkRequest();
    for (String ip2port : ip2portList) {
      String[] ipAndPort = ip2port.split(":");
      request.add(new IndexRequest(ES_INDEX, ES_TYPE, ip2port)
          .source(String.format(Model.proxyModel, ipAndPort[0],
              Integer.valueOf(ipAndPort[1])), XContentType.JSON));
    }
    try {
      client.bulk(request, RequestOptions.DEFAULT);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      closeClient();
    }
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
    String proxyModel = "{" + "\"" + ip + "\":\"%s\"," + "\"" + port
        + "\":\"%s\"," + "\"" + score + "\":0," + "\"" + time + "\":\""
        + new Date().toString() + "\"" + "}";
  }
  
  public static void main(String[] args)
      throws Exception, InterruptedException {
	  JSONObject jsonObject = new JSONObject();
	  jsonObject.put("url", "rereff");
	  jsonObject.put("web", "ewe");
	  System.out.println(jsonObject.toString().split("url\":\"")[1].split("\",\"web")[0]);
  }
  
}
