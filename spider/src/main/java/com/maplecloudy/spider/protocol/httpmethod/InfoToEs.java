package com.maplecloudy.spider.protocol.httpmethod;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

public class InfoToEs {

	private static String ES_IP = "es1.ali.szol.bds.com";
	private final static int ES_PORT = 9200;
	
	private final static String ES_INDEX_HTTP_REEOE = "http_error";
	private final static String ES_INDEX_HTTP_RESPONSE = "http_response";
	private final static String ES_INDEX_PARSE_REEOE = "parse_error";
	private final static String ES_INDEX_PARSE_RESPONSE = "parse_response";
	private final static String ES_INDEX_URL_TYPE = "url_type";


	private final static String ES_TYPE = "_doc";
	
	private final static int BULK_SIZE = 1000;
	
	private final static Object OBJECT = new Object();
	private final static Gson gson = new Gson();
	private static List<String> httpErrorList = Lists.newArrayList();
	private static List<String> httpResponseList = Lists.newArrayList();
	private static List<String> parseErrorList = Lists.newArrayList();
	private static List<String> parseResponseList = Lists.newArrayList();
	private static List<String> urlTypeList = Lists.newArrayList();

	private static boolean flag = false;
	
	private RestHighLevelClient client;
	private static InfoToEs infoToEs;

	private InfoToEs(){}
	
	private InfoToEs(Configuration conf) {
	}

	public static InfoToEs getInstance() {
		if (infoToEs != null)
			return infoToEs;
		synchronized (OBJECT) {
			if (infoToEs == null) {
				infoToEs = new InfoToEs();
			}
		}
		return infoToEs;
	}
	
	public static InfoToEs getInstance(Configuration conf) {
		if (infoToEs != null)
			return infoToEs;
		synchronized (OBJECT) {
			if (infoToEs == null) {
				infoToEs = new InfoToEs(conf);
				infoToEs.client = new RestHighLevelClient(
						RestClient.builder(new HttpHost(ES_IP, ES_PORT, "http")));
			}
		}
		return infoToEs;
	}
	
	private static String HTTP_ERROR_MODEL = "{\"url\":\"%s\",\"code\":%s,\"error\":\"%s\",\"time\":%s}";
	public synchronized void addHttpError(String url,int code, Exception e) {
		StringBuffer message = new StringBuffer();
		StackTraceElement[] exceptionStack = e.getStackTrace();
		message.append(e.getMessage());
		for (StackTraceElement ste : exceptionStack) {
			message.append("\n\tat " + ste);
		}
		httpErrorList.add(String.format(HTTP_ERROR_MODEL, url, code, message.toString(), System.currentTimeMillis()));
		if (httpErrorList.size() >= BULK_SIZE) {
			if(client == null) client = new RestHighLevelClient(
					RestClient.builder(new HttpHost(ES_IP, ES_PORT, "http")));
			BulkRequest request = new BulkRequest();
			for (String error : httpErrorList) {
				request.add(new IndexRequest(ES_INDEX_HTTP_REEOE, ES_TYPE).source(error, XContentType.JSON));
			}
			try {
				client.bulk(request, RequestOptions.DEFAULT);
			} catch (Exception e1) {
				e1.printStackTrace();
			} finally {
				httpErrorList.clear();
//				closeClient();
			}
		}	
	}
	
	private static String PARSE_ERROR_MODEL = "{\"url\":\"%s\",\"error\":\"%s\",\"time\":%s}";
	public synchronized void addParseError(String url,Exception e) {
		StringBuffer message = new StringBuffer();
		StackTraceElement[] exceptionStack = e.getStackTrace();
		message.append(e.getMessage());
		for (StackTraceElement ste : exceptionStack) {
			message.append("\n\tat " + ste);
		}
		parseErrorList.add(String.format(PARSE_ERROR_MODEL, url, message.toString(), System.currentTimeMillis()));
		if (parseErrorList.size() >= BULK_SIZE) {
			if(client == null) client = new RestHighLevelClient(
					RestClient.builder(new HttpHost(ES_IP, ES_PORT, "http")));
			BulkRequest request = new BulkRequest();
			for (String error : parseErrorList) {
				request.add(new IndexRequest(ES_INDEX_PARSE_REEOE, ES_TYPE).source(error, XContentType.JSON));
			}
			try {
				client.bulk(request, RequestOptions.DEFAULT);
			} catch (Exception e1) {
				e1.printStackTrace();
			} finally {
				parseErrorList.clear();
//				closeClient();
			}
		}	
	}
	
	private static String HTTP_RESPONSE_MODEL = "{\"url\":\"%s\",\"code\":%s,\"response\":\"%s\",\"time\":%s}";
	public synchronized void addHttpResponse(String url,int code, String response) {
		httpResponseList.add(String.format(HTTP_RESPONSE_MODEL, url, code, response, System.currentTimeMillis()));
		if (httpResponseList.size() >= BULK_SIZE) {
			if(client == null) client = new RestHighLevelClient(
					RestClient.builder(new HttpHost(ES_IP, ES_PORT, "http")));
			BulkRequest request = new BulkRequest();
			for (String info : httpResponseList) {
				request.add(new IndexRequest(ES_INDEX_HTTP_RESPONSE, ES_TYPE).source(info, XContentType.JSON));
			}
			try {
				client.bulk(request, RequestOptions.DEFAULT);
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				httpResponseList.clear();
//				closeClient();
			}
		}	
	}
	
	
	private static String PARSE_RESPONSE_MODEL = "{\"url\":\"%s\",\"response\":%s,\"time\":%s}";
	public synchronized void addParseResponse(String url,List<Object> response) {
		parseResponseList.add(String.format(PARSE_RESPONSE_MODEL, url, gson.toJson(response), System.currentTimeMillis()));
		if (parseResponseList.size() >= BULK_SIZE) {
			if(client == null) client = new RestHighLevelClient(
					RestClient.builder(new HttpHost(ES_IP, ES_PORT, "http")));
			BulkRequest request = new BulkRequest();
			for (String info : parseResponseList) {
				request.add(new IndexRequest(ES_INDEX_PARSE_RESPONSE, ES_TYPE).source(info, XContentType.JSON));
			}
			try {
				client.bulk(request, RequestOptions.DEFAULT);
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				parseResponseList.clear();
//				closeClient();
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
	
	private static String UTL_TYPE_MODEL = "{\"url\":\"%s\",\"web\":\"%s\",\"type\":\"%s\",\"retry\":%s,\"parse\":\"%s\"}";
	public synchronized void addUrlType(String url, String web, String type, String parse) {
		urlTypeList.add(String.format(UTL_TYPE_MODEL, url, web, type, 1, parse));
		if (urlTypeList.size() >= BULK_SIZE) {
			if(client == null) client = new RestHighLevelClient(
					RestClient.builder(new HttpHost(ES_IP, ES_PORT, "http")));
			BulkRequest request = new BulkRequest();
			for (String info : urlTypeList) {
				Map<String, Object> parameters = Maps.newHashMap();
				parameters.put(UrlTypeModel.retry, 1);
				Script inline = new Script(ScriptType.INLINE, "painless", "ctx._source."+ UrlTypeModel.retry +" += params." + UrlTypeModel.retry, parameters);  
				request.add(new UpdateRequest(ES_INDEX_URL_TYPE, ES_TYPE, info.split("url\":\"")[1].split("\",\"web")[0]).upsert(info, XContentType.JSON).id(info.split("url\":\"")[1].split("\",\"web")[0]).script(inline));
			}
			try {
				client.bulk(request, RequestOptions.DEFAULT);
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				urlTypeList.clear();
//				closeClient();
			}
		}	
	}
	
	public synchronized void cleanUp() {
		if(client == null) return;
		if (flag) return;
		client = new RestHighLevelClient(
				RestClient.builder(new HttpHost(ES_IP, ES_PORT, "http")));
		BulkRequest request = new BulkRequest();
		for (String info : parseResponseList) {
			request.add(new IndexRequest(ES_INDEX_PARSE_RESPONSE, ES_TYPE).source(info, XContentType.JSON));
		}
		for (String info : httpResponseList) {
			request.add(new IndexRequest(ES_INDEX_HTTP_RESPONSE, ES_TYPE).source(info, XContentType.JSON));
		}
		for (String error : parseErrorList) {
			request.add(new IndexRequest(ES_INDEX_PARSE_REEOE, ES_TYPE).source(error, XContentType.JSON));
		}
		for (String error : httpErrorList) {
			request.add(new IndexRequest(ES_INDEX_HTTP_REEOE, ES_TYPE).source(error, XContentType.JSON));
		}
		try {
			client.bulk(request, RequestOptions.DEFAULT);
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
			client.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
}
