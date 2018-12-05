package com.maplecloudy.spider.parse;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.reflect.Nullable;

import com.maplecloudy.avro.util.AvroUtils;
import com.maplecloudy.spider.net.BasicURLNormalizer;

/* An outgoing link from a page. */
public class Outlink {
  
  public String url;
  @Nullable
  public String anchor;
  public int fetchInterval;
  @Nullable
  public Map<String,String> extend;
  
  public Outlink() {}
  
  public Outlink(String url, String anchor) throws MalformedURLException {
    if (url == null) this.url = "";
    this.url = new BasicURLNormalizer().normalize(url);
    if (anchor == null) this.anchor = "";
    this.anchor = anchor;
  }
  
  public String getUrl() {
    return this.url.toString();
  }
  
  public void setUrl(String url) {
    this.url =url;
  }
  
  public String getAnchor() {
    return this.anchor.toString();
  }
  
  public void setAnchor(String anchor) {
    this.anchor = anchor;
  }
  
  public int getFetchInterval() {
    return this.fetchInterval;
  }
  
  public void setFetchInterval(int fetchInterval) {
    this.fetchInterval = fetchInterval;
  }
  
  public Map<String,String> getExtend() {
    if (this.extend == null) this.extend = new HashMap<String,String>();
    return this.extend;
  }
  
  public void setExtend(
      Map<String,String> extend) {
    this.extend = extend;
  }
  
  public void addExtend(
      Map<String,String> extend) {
    this.getExtend().putAll(extend);
  }
  
  public void addExtend(String name, String value) {
    this.getExtend().put(name, value);
  }
  
  public String getExtend(String key) {
    if (getExtend().get(key) != null) return getExtend().get(key).toString();
    else return null;
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof Outlink) {
      if (this.url != null
          && this.url.equals(((Outlink) o).url)) return true;
      else return false;
    } else return false;
  }
  
  @Override
  public String toString() {
    return AvroUtils.toAvroString(this);
  }
}
