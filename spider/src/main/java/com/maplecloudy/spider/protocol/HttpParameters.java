package com.maplecloudy.spider.protocol;

import java.util.Map;

import com.google.common.collect.Maps;

public class HttpParameters {
  private Map<String,String> map;
  private String method; // 访问方法 jsoup http
  private String type; // 访问类型:post get
  // http header
  private String cookie;
  private String userAgent;
  private String accept;
  private String contentType;
  private String requestBody; // post 请求体
  private String ignoreContentType;
  private String x_requested_with;
  private String refer;
  private String accept_encoding;
  private String accept_language;
  private String timeout;
  private String charset;
  private String validateTLSCertificates;
  
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
    if (!map.containsKey("method")) return "jsoup";
    return map.get("method");
  }
  
  public void setMethod(String method) {
    map.put("method", method);
  }
  
  public String getType() {
    if (!map.containsKey("type")) return "get";
    return map.get("type");
  }
  
  public void setType(String type) {
    map.put("type", type);
  }
  
  public String getCookie() {
    if (!map.containsKey("cookie")) return null;
    return map.get("cookie");
  }
  
  public void setCookie(String cookie) {
    map.put("cookie", cookie);
  }
  
  public String getUserAgent() {
    if (!map.containsKey("userAgent")) return null;
    return map.get("userAgent");
  }
  
  public void setUserAgent(String userAgent) {
    map.put("userAgent", userAgent);
  }
  
  public String getAccept() {
    if (!map.containsKey("accept")) return null;
    return map.get("accept");
  }
  
  public void setAccept(String accept) {
    map.put("accept", accept);
  }
  
  public String getContentType() {
    if (!map.containsKey("contentType")) return null;
    return map.get("contentType");
  }
  
  public void setContentType(String contentType) {
    map.put("contentType", contentType);
  }
  
  public String getRequestBody() {
    if (!map.containsKey("requestBody")) return null;
    return map.get("requestBody");
  }
  
  public void setRequestBody(String requestBody) {
    map.put("requestBody", requestBody);
  }
  
  public String getIgnoreContentType() {
    if (!map.containsKey("ignoreContentType")) return null;
    return map.get("ignoreContentType");
  }
  
  public void setIgnoreContentType(String ignoreContentType) {
    map.put("ignoreContentType", ignoreContentType);
  }
  
  public String getX_requested_with() {
    if (!map.containsKey("x_requested_with")) return null;
    return map.get("x_requested_with");
  }
  
  public void setX_requested_with(String x_requested_with) {
    map.put("x_requested_with", x_requested_with);
  }
  
  public String getRefer() {
    if (!map.containsKey("refer")) return null;
    return map.get("refer");
  }
  
  public void setRefer(String refer) {
    map.put("refer", refer);
  }
  
  public String getAccept_encoding() {
    if (!map.containsKey("accept_encoding")) return null;
    return map.get("accept_encoding");
  }
  
  public void setAccept_encoding(String accept_encoding) {
    map.put("accept_encoding", accept_encoding);
  }
  
  public String getAccept_language() {
    if (!map.containsKey("accept_language")) return null;
    return map.get("accept_language");
  }
  
  public void setAccept_language(String accept_language) {
    map.put("accept_language", accept_language);
  }
  
  public String getTimeout() {
    if (!map.containsKey("timeout")) return "10000";
    return map.get("timeout");
  }
  
  public void setTimeout(String timeout) {
    map.put("timeout", timeout);
  }
  
  public String getCharset() {
    if (!map.containsKey("charset")) return null;
    return map.get("charset");
  }
  
  public void setCharset(String charset) {
    map.put("charset", charset);
  }
  
  public String getValidateTLSCertificates() {
    if (!map.containsKey("validateTLSCertificates")) return null;
    return map.get("validateTLSCertificates");
  }
  
  public void setValidateTLSCertificates(String validateTLSCertificates) {
    map.put("validateTLSCertificates", validateTLSCertificates);
  }
  
}
