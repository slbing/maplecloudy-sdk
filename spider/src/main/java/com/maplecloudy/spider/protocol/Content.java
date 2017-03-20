package com.maplecloudy.spider.protocol;

//JDK imports
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class Content {
  
  public static final String DIR_NAME = "content";
  
  public String url;
  public ByteBuffer content;
  public Map<String,String> extend;
  public Map<String,List<String>> metaData;
  
  public Content() {}
  
  public Content(String url, byte[] content, Map<String,List<String>> metaData) {
    
    if (url == null) throw new IllegalArgumentException("null url");
    if (content == null) throw new IllegalArgumentException("null content");
    
    this.url = url;
    this.content = ByteBuffer.wrap(content);
    this.setMetadata(metaData);
  }
  
  /** The url fetched. */
  public String getUrl() {
    return url.toString();
  }
  
  /** The binary content retrieved. */
  public byte[] getContent() {
    if (content != null) return content.array();
    else return null;
  }
  
  public void setContent(byte[] content) {
    this.content = ByteBuffer.wrap(content);
  }
  
  public Map<String,List<String>> getMetadata() {
    if (metaData == null) metaData = new HashMap<String,List<String>>();
    return metaData;
  }
  
  public void setMetadata(Map<String,List<String>> metaData) {
    this.metaData = metaData;
  }
  
  public static void main(String argv[]) throws Exception {
    
  }
  
  public void addMetadata(String name, String value) {
    List<String> val = getMetadata().remove(name);
    if (val == null) val = new ArrayList<String>();
    val.add(value);
    getMetadata().put(name, val);
  }
  
  public void addMetadata(String name, String[] value) {
    List<String> val = getMetadata().remove(name);
    if (val == null) val = new ArrayList<String>();
    for (String s : value) {
      val.add(s);
    }
    getMetadata().put(name, val);
  }
  
  public String getMeta(String name) {
    List<String> val = getMetadata().get(name);
    if (val == null || val.size() < 1) return null;
    else return val.get(0).toString();
  }
  
  public Map<String,String> getExtendData() {
    if (extend == null) extend = new HashMap<String,String>();
    return extend;
  }
  
  public void setExtendData(Map<String,String> extend) {
    this.extend = extend;
  }
  
  public void setExtend(String key, String value) {
    getExtendData().put(key, value);
  }
  
  public String getExtend(String key) {
    return getExtendData().get(key).toString();
  }
  
}