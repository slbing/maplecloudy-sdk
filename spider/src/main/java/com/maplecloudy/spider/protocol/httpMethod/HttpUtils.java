package com.maplecloudy.spider.protocol.httpMethod;

import java.lang.invoke.MethodHandles;

import org.apache.hadoop.conf.Configuration;
import org.jsoup.Connection;
import org.jsoup.Connection.Method;
import org.jsoup.Jsoup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.maplecloudy.spider.crawl.CrawlDatum;
import com.maplecloudy.spider.protocol.Content;
import com.maplecloudy.spider.protocol.HttpParameters;
import com.maplecloudy.spider.protocol.Protocol;
import com.maplecloudy.spider.protocol.ProtocolOutput;
import com.maplecloudy.spider.protocol.ProtocolStatus;

public class HttpUtils implements Protocol {
	
	private static final Logger LOG = LoggerFactory
	          .getLogger(MethodHandles.lookup().lookupClass());
	
	private final static int TIME_OUT = 10 * 1000;
	private static final byte[] EMPTY_CONTENT = new byte[0];
	private Configuration conf = null;


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
		System.out.println(datum.getExtendData().toString());
		try {
			HttpParameters parm = new HttpParameters(datum.getExtendData());
			if ("http".equals(parm.getMethod())) {
				
				return null;
			} else {
				Connection.Response response;
				if ("post".equals(parm.getType())) {
					Connection connection = Jsoup.connect(url)
							.timeout(TIME_OUT)
							.ignoreContentType(true)
							.method(Method.POST)
							.header("User-Agent", UserAgent.getAgent());
					if (parm.getAccept() != null) connection.header("Accept", parm.getAccept());
					if (parm.getAccept_encoding() != null) connection.header("Accept-Encoding", parm.getAccept_encoding()); 
					if (parm.getAccept_language() != null) connection.header("Accept-Language", parm.getAccept_language()); 
					if (parm.getCookie() != null) connection.header("Cookie", parm.getCookie()); 
					if (parm.getX_requested_with() != null) connection.header("x_requested_with", parm.getX_requested_with()); 
					if (parm.getContentType() != null) connection.header("Content-Type", parm.getContentType()); 
					if (parm.getRequestBody() != null)  connection.requestBody(parm.getRequestBody());
					response = connection.execute();
				} else {
					Connection connection = Jsoup.connect(url)
							.timeout(TIME_OUT)
							.ignoreContentType(true)
							.method(Method.GET)
							.header("User-Agent", UserAgent.getAgent());
					if (parm.getAccept() != null) connection.header("Accept", parm.getAccept());
					if (parm.getAccept_encoding() != null) connection.header("Accept-Encoding", parm.getAccept_encoding()); 
					if (parm.getAccept_language() != null) connection.header("Accept-Language", parm.getAccept_language()); 
					if (parm.getCookie() != null) connection.header("Cookie", parm.getCookie()); 
					if (parm.getX_requested_with() != null) connection.header("x_requested_with", parm.getX_requested_with()); 
					if (parm.getContentType() != null) connection.header("Content-Type", parm.getContentType()); 
					response = connection.execute();	
				}
				int code = response.statusCode();
				byte[] content = response.bodyAsBytes();
				Content c = new Content(url,(content == null ? EMPTY_CONTENT : content),Maps.newConcurrentMap());
				c.setExtendData(datum.getExtendData());
				if (code == 200) { // got a good response
					return new ProtocolOutput(c); // return it

				} else if (code == 410) { // page is gone
					return new ProtocolOutput(c, new ProtocolStatus(
							ProtocolStatus.GONE, "Http: " + code + " url=" + url));

				} else if (code == 400) { // bad request, mark as GONE
					if (LOG.isTraceEnabled()) {
						LOG.trace("400 Bad request: " + url);
					}
					return new ProtocolOutput(c, new ProtocolStatus(
							ProtocolStatus.GONE, url));
				} else if (code == 401) { // requires authorization, but no valid
											// auth provided.
					if (LOG.isTraceEnabled()) {
						LOG.trace("401 Authentication Required");
					}
					return new ProtocolOutput(c, new ProtocolStatus(
							ProtocolStatus.ACCESS_DENIED,
							"Authentication required: " + url));
				} else if (code == 404) {
					return new ProtocolOutput(c, new ProtocolStatus(
							ProtocolStatus.NOTFOUND, url));
				} else if (code == 410) { // permanently GONE
					return new ProtocolOutput(c, new ProtocolStatus(
							ProtocolStatus.GONE, url));
				} else {
					return new ProtocolOutput(c, new ProtocolStatus(
							ProtocolStatus.EXCEPTION, "Http code=" + code
									+ ", url=" + url));
				}
			}
		} catch (Exception e) {
			LOG.error("fetch url" + url + " error", e);
			return new ProtocolOutput(null, new ProtocolStatus(e));
		}
	}

	@Override
	public ProtocolOutput getProtocolOutput(String url) {
		// TODO Auto-generated method stub
		return null;
	}
	
	

}
