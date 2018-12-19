package com.maplecloudy.spider.protocol.httpmethod;

import java.lang.invoke.MethodHandles;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.http.HttpHost;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.jsoup.Connection;
import org.jsoup.Connection.Method;
import org.jsoup.Jsoup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.maplecloudy.spider.crawl.CrawlDatum;
import com.maplecloudy.spider.parse.Outlink;
import com.maplecloudy.spider.protocol.Content;
import com.maplecloudy.spider.protocol.HttpParameters;
import com.maplecloudy.spider.protocol.Protocol;
import com.maplecloudy.spider.protocol.ProtocolOutput;
import com.maplecloudy.spider.protocol.ProtocolStatus;

public class HttpUtils implements Protocol {

	private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	private final static int TIME_OUT = 5 * 1000;
	private final static int MAX_SIZE = 10 * 1024 * 1024;

	private static CloseableHttpClient httpClient = HttpClients.createDefault();
	private static RequestConfig.Builder builder = RequestConfig.custom().setSocketTimeout(TIME_OUT)
			.setConnectTimeout(TIME_OUT).setConnectionRequestTimeout(TIME_OUT)
			.setCookieSpec(CookieSpecs.IGNORE_COOKIES);
	
	public static boolean ES_ABLE = true;

	private static final byte[] EMPTY_CONTENT = new byte[0];
	private Configuration conf = null;
	private static HttpUtils httpUtils;
	private final static Object OBJECT = new Object();

	private HttpUtils() {
	}

	public static HttpUtils getInstance() {
		if (httpUtils != null)
			return httpUtils;
		synchronized (OBJECT) {
			if (httpUtils == null) {
				httpUtils = new HttpUtils();
			}
		}
		return httpUtils;
	}

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return conf;
	}

	@Override
	public void setConf(Configuration arg0) {
		this.conf = arg0;
	}

	@Override
	public ProtocolOutput getProtocolOutput(String url, CrawlDatum datum) {
		// TODO Auto-generated method stub
		try {
			HttpParameters parm = new HttpParameters(datum.getExtendData());
			String ip2port = ProxyWithEs.getInstance().getProxy();
			Content c;
			int code;
			String html = "";
			if ("http".equals(parm.getMethod())) {
				HttpRequestBase http = null;
				if ("post".equals(parm.getType())) {
					http = new HttpPost(url);
				} else {
					http = new HttpGet(url);
				}
				if (parm.getAccept() != null)
					http.addHeader("Accept", parm.getAccept());
				if (parm.getAccept_encoding() != null)
					http.addHeader("Accept-Encoding", parm.getAccept_encoding());
				if (parm.getAccept_language() != null)
					http.addHeader("Accept-Language", parm.getAccept_language());
				if (parm.getCookie() != null)
					http.addHeader("Cookie", parm.getCookie());
				if (parm.getX_requested_with() != null)
					http.addHeader("x_requested_with", parm.getX_requested_with());
				if (parm.getContentType() != null)
					http.addHeader("Content-Type", parm.getContentType());
			
				if (ip2port != null) {
					String[] proxy = ip2port.split(":");
					http.setConfig(builder.setProxy(new HttpHost(proxy[0], Integer.valueOf(proxy[1]))).build());
				}
				CloseableHttpResponse response;
				try {
					response = httpClient.execute(http);
				} catch (Exception e) {					
					if (ES_ABLE) InfoToEs.getInstance().addHttpError(url, 900, e);
					http.setConfig(builder.build());
					response = httpClient.execute(http);
				}
				code = response.getStatusLine().getStatusCode();
				String chareset = "utf-8";
				if (parm.getCharset() != null)
					chareset = parm.getCharset();
				html = EntityUtils.toString(response.getEntity(), chareset);
				response.close();
				byte[] content = html.getBytes();
				c = new Content(url, (content == null ? EMPTY_CONTENT : content), Maps.newHashMap());
				c.setExtendData(datum.getExtendData());
			  c = new Content(url,(EMPTY_CONTENT),Maps.newHashMap());
			  code=0;
			} else {
				Connection connection = Jsoup.connect(url).timeout(TIME_OUT).maxBodySize(MAX_SIZE)
						.ignoreContentType(true).userAgent(UserAgent.getAgent());
				if (parm.getAccept() != null)
					connection.header("Accept", parm.getAccept());
				if (parm.getAccept_encoding() != null)
					connection.header("Accept-Encoding", parm.getAccept_encoding());
				if (parm.getAccept_language() != null)
					connection.header("Accept-Language", parm.getAccept_language());
				if (parm.getCookie() != null)
					connection.header("Cookie", parm.getCookie());
				if (parm.getX_requested_with() != null)
					connection.header("x_requested_with", parm.getX_requested_with());
				if (parm.getContentType() != null)
					connection.header("Content-Type", parm.getContentType());
				if (ip2port != null) {
					String[] proxy = ip2port.split(":");
					connection.proxy(new Proxy(Proxy.Type.HTTP, InetSocketAddress.createUnresolved(proxy[0], Integer.valueOf(proxy[1]))));
				}

				if ("post".equals(parm.getType())) {
					connection.method(Method.POST);
					if (parm.getRequestBody() != null)
						connection.requestBody(parm.getRequestBody());
				} else {
					connection = connection.method(Method.GET);
				}
				Connection.Response response;
				try {
					response = connection.execute();
				} catch (Exception e) {
					if (ES_ABLE) InfoToEs.getInstance().addHttpError(url, 901, e);
					connection.proxy(null);
					response = connection.execute();
				}
				code = response.statusCode();
				html = response.body();
				byte[] content = response.bodyAsBytes();
				c = new Content(url, (content == null ? EMPTY_CONTENT : content), Maps.newHashMap());
				c.setExtendData(datum.getExtendData());
			}
			if (ES_ABLE) InfoToEs.getInstance().addHttpResponse(url, code, html);
			if (code == 200) { // got a good response
				return new ProtocolOutput(c); // return it

			} else if (code == 410) { // page is gone
				return new ProtocolOutput(c, new ProtocolStatus(ProtocolStatus.GONE, "Http: " + code + " url=" + url));

			} else if (code == 400) { // bad request, mark as GONE
				if (LOG.isTraceEnabled()) {
					LOG.trace("400 Bad request: " + url);
				}
				return new ProtocolOutput(c, new ProtocolStatus(ProtocolStatus.GONE, url));
			} else if (code == 401) { // requires authorization, but no valid
										// auth provided.
				if (LOG.isTraceEnabled()) {
					LOG.trace("401 Authentication Required");
				}
				return new ProtocolOutput(c,
						new ProtocolStatus(ProtocolStatus.ACCESS_DENIED, "Authentication required: " + url));
			} else if (code == 404) {
				return new ProtocolOutput(c, new ProtocolStatus(ProtocolStatus.NOTFOUND, url));
			} else if (code == 410) { // permanently GONE
				return new ProtocolOutput(c, new ProtocolStatus(ProtocolStatus.GONE, url));
			} else {
				return new ProtocolOutput(c,
						new ProtocolStatus(ProtocolStatus.EXCEPTION, "Http code=" + code + ", url=" + url));
			}
		} catch (Exception e) {
			LOG.error("fetch -- url " + url + " error " , e);
			if (ES_ABLE) InfoToEs.getInstance().addHttpError(url, 0, e);
			return new ProtocolOutput(null, new ProtocolStatus(e));
		}
	}

	@Override
	public ProtocolOutput getProtocolOutput(String url) {
		return getProtocolOutput(url, new CrawlDatum());
	}
	
	public class Model {
		String string;
	}

	public static void main(String[] args) throws Exception {
		RestHighLevelClient client = new RestHighLevelClient(
				RestClient.builder(new HttpHost("localhost", 9200, "http")));
		BulkRequest request = new BulkRequest();
		Map<String, Object> parameters = Maps.newHashMap();
		parameters.put("retry", 1);
		Script inline = new Script(ScriptType.INLINE, "painless", "ctx._source.retry+= params.retry", parameters);  
		request.add(new UpdateRequest("urltype", "_doc", "wwwe.ee").upsert("{\"url\":\"wwwe.ee\",\"retry\":1}", XContentType.JSON).id("wwwe.ee").script(inline));
		client.bulk(request, RequestOptions.DEFAULT);
		client.close();
	}

}
