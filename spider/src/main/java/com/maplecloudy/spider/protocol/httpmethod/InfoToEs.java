package com.maplecloudy.spider.protocol.httpmethod;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig.Builder;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

public class InfoToEs {
  
  private final static String ES_IP = "localhost";
  private final static int ES_PORT = 9200;
  private final static String ES_HOST = "es1.ali.szol.bds.com:9200,es2.ali.szol.bds.com:9200";
  
  private final static String ES_INDEX_HTTP_REEOE = "http_error";
  private final static String ES_INDEX_HTTP_RESPONSE = "http_response";
  private final static String ES_INDEX_PARSE_REEOE = "parse_error";
  private final static String ES_INDEX_PARSE_RESPONSE = "parse_response";
  private final static String ES_INDEX_URL_TYPE = "url_type";
  
  private final static String ES_TYPE = "_doc";
  
  private final static int BULK_SIZE = 500;
  
  private final static Object OBJECT = new Object();
  private final static Gson gson = new Gson();
  private static List<String> httpErrorList = Lists.newArrayList();
  private static List<String> httpResponseList = Lists.newArrayList();
  private static List<String> parseErrorList = Lists.newArrayList();
  private static List<String> parseResponseList = Lists.newArrayList();
  private static List<String> urlTypeList = Lists.newArrayList();
  
  private volatile static boolean flag = false;
  private final static JSONObject json = new JSONObject();
  private final static List<String> listKey = Lists.newArrayList();
  
  private volatile RestHighLevelClient client;
  private static InfoToEs infoToEs;
  private volatile Configuration conf;
  
  private InfoToEs() {}
  
  public static InfoToEs getInstance() {
    if (infoToEs != null) return infoToEs;
    synchronized (OBJECT) {
      if (infoToEs == null) {
        infoToEs = new InfoToEs();
      }
    }
    return infoToEs;
  }
  
  public static InfoToEs getInstance(Configuration conf) {
    if (infoToEs != null) return infoToEs;
    synchronized (OBJECT) {
      if (infoToEs == null) {
        infoToEs = new InfoToEs();
        infoToEs.conf = conf;
      }
    }
    return infoToEs;
  }
  
  public void initClient() {
    if (this.client != null) return;
    if (this.conf == null) this.conf = new Configuration();
    String clusterNodes = this.conf.getStrings("es.hosts", ES_HOST)[0];
    ArrayList<HttpHost> hosts = Lists.newArrayList();
    for (String clusterNode : clusterNodes.split(",")) {
      String hostName = clusterNode.split(":")[0];
      String port = clusterNode.split(":")[1];
      hosts.add(new HttpHost(hostName, Integer.valueOf(port)));
    }
    this.client = new RestHighLevelClient(
        RestClient.builder(hosts.toArray(new HttpHost[hosts.size()]))
            .setRequestConfigCallback(
                new RestClientBuilder.RequestConfigCallback() {
                  @Override
                  public Builder customizeRequestConfig(
                      Builder requestConfigBuilder) {
                    // 请超时
                    return requestConfigBuilder.setConnectTimeout(5000)
                        .setSocketTimeout(10000)
                        .setConnectionRequestTimeout(10000);
                  }
                })
            .setMaxRetryTimeoutMillis(10000));
  }
  
  public synchronized void addHttpError(String url, int code, String web,
      String type, String urlType, String pageNum, String deepth, Exception e) {
    
    try {
      json.put("url", url);
      json.put("code", code);
      json.put("web", web);
      json.put("type", type);
      json.put("urltype", urlType);
      json.put("pageNum", Integer.valueOf(pageNum));
      json.put("deepth", Integer.valueOf(deepth));
      json.put("error", StringUtils.stringifyException(e));
      json.put("time", System.currentTimeMillis());
      httpErrorList.add(json.toString());
    } catch (JSONException e2) {
      e2.printStackTrace();
    } finally {
      cleanData();
    }
    if (httpErrorList.size() >= BULK_SIZE) {
      BulkRequest request = new BulkRequest();
      for (String error : httpErrorList) {
        request.add(new IndexRequest(ES_INDEX_HTTP_REEOE, ES_TYPE).source(error,
            XContentType.JSON));
      }
      try {
        if (this.client != null)
          this.client.bulk(request, RequestOptions.DEFAULT);
      } catch (Exception e1) {
        e1.printStackTrace();
      } finally {
        httpErrorList.clear();
      }
    }
  }
  
  public synchronized void addParseError(String url, String web, String type,
      String urlType, Exception e) {
    try {
      json.put("url", url);
      json.put("web", web);
      json.put("type", type);
      json.put("urltype", urlType);
      json.put("error", StringUtils.stringifyException(e));
      json.put("time", System.currentTimeMillis());
      parseErrorList.add(json.toString());
    } catch (JSONException e2) {
      e2.printStackTrace();
    } finally {
      cleanData();
    }
    parseErrorList.add(json.toString());
    if (parseErrorList.size() >= BULK_SIZE) {
      BulkRequest request = new BulkRequest();
      for (String error : parseErrorList) {
        request.add(new IndexRequest(ES_INDEX_PARSE_REEOE, ES_TYPE)
            .source(error, XContentType.JSON));
      }
      try {
        if (this.client != null)
          this.client.bulk(request, RequestOptions.DEFAULT);
      } catch (Exception e1) {
        e1.printStackTrace();
      } finally {
        parseErrorList.clear();
      }
    }
  }
  
  public synchronized void addHttpResponse(String url, int code, String web,
      String type, String urlType, String pageNum, String deepth,
      String response) {
    try {
      json.put("url", url);
      json.put("web", web);
      json.put("type", type);
      json.put("urltype", urlType);
      json.put("pageNum", Integer.valueOf(pageNum));
      json.put("deepth", Integer.valueOf(deepth));
      json.put("code", code);
      json.put("response", response);
      json.put("time", System.currentTimeMillis());
      httpResponseList.add(json.toString());
    } catch (JSONException e2) {
      e2.printStackTrace();
    } finally {
      cleanData();
    }
    if (httpResponseList.size() >= BULK_SIZE / 2) {
      BulkRequest request = new BulkRequest();
      for (String info : httpResponseList) {
        request.add(new IndexRequest(ES_INDEX_HTTP_RESPONSE, ES_TYPE)
            .source(info, XContentType.JSON));
      }
      try {
        if (this.client != null)
          this.client.bulk(request, RequestOptions.DEFAULT);
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        httpResponseList.clear();
      }
    }
  }
  
  public synchronized void addParseResponse(String url, String web, String type,
      String urlType, String pageNum, String deepth, List<Object> response) {
    try {
      json.put("url", url);
      json.put("web", web);
      json.put("type", type);
      json.put("urltype", urlType);
      json.put("pageNum", Integer.valueOf(pageNum));
      json.put("deepth", Integer.valueOf(deepth));
      json.put("size", response.size());
      json.put("response", response.stream().map(e -> gson.toJson(e))
          .collect(Collectors.toList()));
      json.put("time", System.currentTimeMillis());
      parseResponseList.add(json.toString());
    } catch (JSONException e2) {
      e2.printStackTrace();
    } finally {
      cleanData();
    }
    if (parseResponseList.size() >= BULK_SIZE) {
      BulkRequest request = new BulkRequest();
      for (String info : parseResponseList) {
        request.add(new IndexRequest(ES_INDEX_PARSE_RESPONSE, ES_TYPE)
            .source(info, XContentType.JSON));
      }
      try {
        if (this.client != null)
          this.client.bulk(request, RequestOptions.DEFAULT);
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        parseResponseList.clear();
      }
    }
  }
  
  private interface UrlTypeModel {
    String url = "url";
    String web = "web";
    String type = "type";
    String retry = "retry";
    String parse = "parse";
    
  }
  
  public synchronized void addUrlType(String url, String web, String type,
      String urlType, String pageNum, String deepth, String parse) {
    try {
      json.put("url", url);
      json.put("web", web);
      json.put("type", type);
      json.put("urltype", urlType);
      json.put("pageNum", Integer.valueOf(pageNum));
      json.put("deepth", Integer.valueOf(deepth));
      json.put("parse", parse);
      json.put("retry", 1);
      json.put("time", System.currentTimeMillis());
      urlTypeList.add(json.toString());
    } catch (JSONException e2) {
      e2.printStackTrace();
    } finally {
      cleanData();
    }
    if (urlTypeList.size() >= BULK_SIZE) {
      BulkRequest request = new BulkRequest();
      for (String info : urlTypeList) {
        Map<String,Object> parameters = Maps.newHashMap();
        parameters.put(UrlTypeModel.retry, 1);
        Script inline = new Script(ScriptType.INLINE, "painless", "ctx._source."
            + UrlTypeModel.retry + " += params." + UrlTypeModel.retry,
            parameters);
        request.add(new UpdateRequest(ES_INDEX_URL_TYPE, ES_TYPE,
            info.split("url\":\"")[1].split("\",\"web")[0])
                .upsert(info, XContentType.JSON)
                .id(info.split("url\":\"")[1].split("\",\"web")[0])
                .script(inline));
      }
      try {
        if (this.client != null)
          this.client.bulk(request, RequestOptions.DEFAULT);
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        urlTypeList.clear();
      }
    }
  }
  
  public synchronized void cleanUp() {
    if (client == null) return;
    if (flag) return;
    BulkRequest request = new BulkRequest();
    for (String info : parseResponseList) {
      request.add(new IndexRequest(ES_INDEX_PARSE_RESPONSE, ES_TYPE)
          .source(info, XContentType.JSON));
    }
    for (String info : httpResponseList) {
      request.add(new IndexRequest(ES_INDEX_HTTP_RESPONSE, ES_TYPE).source(info,
          XContentType.JSON));
    }
    for (String error : parseErrorList) {
      request.add(new IndexRequest(ES_INDEX_PARSE_REEOE, ES_TYPE).source(error,
          XContentType.JSON));
    }
    for (String error : httpErrorList) {
      request.add(new IndexRequest(ES_INDEX_HTTP_REEOE, ES_TYPE).source(error,
          XContentType.JSON));
    }
    for (String info : urlTypeList) {
      Map<String,Object> parameters = Maps.newHashMap();
      parameters.put(UrlTypeModel.retry, 1);
      Script inline = new Script(ScriptType.INLINE, "painless", "ctx._source."
          + UrlTypeModel.retry + " += params." + UrlTypeModel.retry,
          parameters);
      request.add(new UpdateRequest(ES_INDEX_URL_TYPE, ES_TYPE,
          info.split("url\":\"")[1].split("\",\"web")[0])
              .upsert(info, XContentType.JSON)
              .id(info.split("url\":\"")[1].split("\",\"web")[0])
              .script(inline));
    }
    try {
      if (!request.requests().isEmpty() && this.client != null)
        this.client.bulk(request, RequestOptions.DEFAULT);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      parseResponseList.clear();
      httpResponseList.clear();
      parseErrorList.clear();
      httpErrorList.clear();
      closeClient();
      flag = true;
    }
  }
  
  private void closeClient() {
    try {
      if (client != null) {
        client.close();
        client = null;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  private void cleanData() {
    Iterator<?> keys = json.keys();
    while (keys.hasNext()) {
      listKey.add((String) keys.next());
    }
    Iterator<String> itKey = listKey.iterator();
    while (itKey.hasNext()) {
      json.remove(itKey.next());
    }
    listKey.clear();
  }
  
}
