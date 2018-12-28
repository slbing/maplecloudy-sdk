package com.maplecloudy.spider.protocol;

import java.util.Map;

import com.google.common.collect.Maps;

public class HttpParameters {
  private Map<String,String> map;
//  private String method = "jsoup"; // 访问方法 jsoup http
//  private String type = "get"; // 访问类型:post get
//  // http header
//  private String cookie;
//  private String userAgent;
//  private String accept;
//  private String contentType;
//  private String requestBody; // post 请求体
//  private String ignoreContentType ;
//  private String x_requested_with ;
//  private String refer;
//  private String accept_encoding ;
//  private String accept_language ;
//  private String timeout ;
//  private String charset ;
//  private String validateTLSCertificates ;
//  
//  private String proxy ;
//  private String proxyIp;
//  private String proxyPort;

  
  public Map<String,String> getMap() {
    return map;
  }
  
  public HttpParameters(Map<String,String> map) {
    this.map = map;
  }
  
  public HttpParameters() {
    Map<String,String> mapIn = Maps.newHashMap();
    this.map = mapIn;
  }
  
  public String getMethod() {
    if (!map.containsKey("http_method")) return "jsoup";
    return map.get("http_method");
  }
  
  public void setMethod(String method) {
    map.put("http_method", method);
  }
  
  public String getType() {
    if (!map.containsKey("http_type")) return "get";
    return map.get("http_type");
  }
  
  public void setType(String type) {
    map.put("http_type", type);
  }
  
  public String getCookie() {
    if (!map.containsKey("http_cookie")) return null;
    return map.get("http_cookie");
  }
  
  public void setCookie(String cookie) {
    map.put("http_cookie", cookie);
  }
  
  public String getUserAgent() {
    if (!map.containsKey("http_userAgent")) return null;
    return map.get("http_userAgent");
  }
  
  public void setUserAgent(String userAgent) {
    map.put("http_userAgent", userAgent);
  }
  
  public String getAccept() {
    if (!map.containsKey("http_accept")) return null;
    return map.get("http_accept");
  }
  
  public void setAccept(String accept) {
    map.put("http_accept", accept);
  }
  
  public String getContentType() {
    if (!map.containsKey("http_contentType")) return null;
    return map.get("http_contentType");
  }
  
  public void setContentType(String contentType) {
    map.put("http_contentType", contentType);
  }
  
  public String getRequestBody() {
    if (!map.containsKey("http_requestBody")) return null;
    return map.get("http_requestBody");
  }
  
  public void setRequestBody(String requestBody) {
    map.put("http_requestBody", requestBody);
  }
  
  public String getIgnoreContentType() {
    if (!map.containsKey("http_ignoreContentType")) return null;
    return map.get("http_ignoreContentType");
  }
  
  public void setIgnoreContentType(String ignoreContentType) {
    map.put("http_ignoreContentType", ignoreContentType);
  }
  
  public String getX_requested_with() {
    if (!map.containsKey("http_x_requested_with")) return null;
    return map.get("http_x_requested_with");
  }
  
  public void setX_requested_with(String x_requested_with) {
    map.put("http_x_requested_with", x_requested_with);
  }
  
  public String getRefer() {
    if (!map.containsKey("http_refer")) return null;
    return map.get("http_refer");
  }
  
  public void setRefer(String refer) {
    map.put("http_refer", refer);
  }
  
  public String getAccept_encoding() {
    if (!map.containsKey("http_accept_encoding")) return null;
    return map.get("http_accept_encoding");
  }
  
  public void setAccept_encoding(String accept_encoding) {
    map.put("http_accept_encoding", accept_encoding);
  }
  
  public String getAccept_language() {
    if (!map.containsKey("http_accept_language")) return null;
    return map.get("http_accept_language");
  }
  
  public void setAccept_language(String accept_language) {
    map.put("http_accept_language", accept_language);
  }
  
  public String getTimeout() {
    if (!map.containsKey("http_timeout")) return "10000";
    return map.get("http_timeout");
  }
  
  public void setTimeout(String timeout) {
    map.put("http_timeout", timeout);
  }
  
  public String getCharset() {
    if (!map.containsKey("http_charset")) return null;
    return map.get("http_charset");
  }
  
  public void setCharset(String charset) {
    map.put("http_charset", charset);
  }
  
  public String getValidateTLSCertificates() {
    if (!map.containsKey("http_validateTLSCertificates")) return null;
    return map.get("http_validateTLSCertificates");
  }
  
  public void setValidateTLSCertificates(String validateTLSCertificates) {
    map.put("http_validateTLSCertificates", validateTLSCertificates);
  }

	public boolean getProxy() {
		if (!map.containsKey("http_proxy")) return false;
	    return Boolean.valueOf(map.get("http_proxy"));
	}
	
	public void setProxy(Boolean proxy) {
		map.put("http_proxy", proxy.toString());
	}
	
	public String getProxyIp() {
		if (!map.containsKey("http_proxyIp")) return "127.0.0.1";
	    return map.get("http_proxyIp");
	}
	
	public void setProxyIp(String proxyIp) {
		map.put("http_proxyIp", proxyIp);
	}
	
	public Integer getProxyPort() {
		if (!map.containsKey("http_proxyPort")) return 12;
	    return Integer.valueOf(map.get("http_proxyPort"));
	}
	
	public void setProxyPort(Integer proxyPort) {
		map.put("http_proxyPort", proxyPort.toString());
	}
  
  
  
}
