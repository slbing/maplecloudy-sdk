package com.maplecloudy.spider.protocol.httpmethod;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.jsoup.Jsoup;

import com.google.common.collect.Lists;

public class Test {

  public static void main(String[] args) throws Exception {
    CloseableHttpClient httpClient = HttpClients.createDefault();
    
    
    while (true) {
      List<String> e = Lists.newArrayList();
      String eString = Jsoup.connect("http://maplecloudy.v4.dailiyun.com/query.txt?key=NPEE573347&word=&count=200&rand=false&detail=false").ignoreContentType(true).get().toString().trim();
      System.out.println(eString);
      eString = eString.split("<body>")[1].split("</body>")[0].trim();
      String[] se = eString.split(" ");
      for (int i = 0; i < se.length; i++) {
        System.out.println(se[i]);
//        e.add(se[i].split(",")[0]);
        try {
          HttpGet http = new HttpGet("https://m.weibo.cn/status/3784111510118507");
          http.setConfig(RequestConfig.custom().setProxy(new HttpHost(se[i].split(":")[0], Integer.valueOf(se[i].split(":")[1]))).build());
          CloseableHttpResponse response = httpClient.execute(http);
          System.out.println(response.getStatusLine().getStatusCode());
          System.out.println(Jsoup.connect("https://m.weibo.cn/status/3784111510118507").timeout(5000).proxy(se[i].split(":")[0], Integer.valueOf(se[i].split(":")[1])).get().title());
        } catch (Exception e2) {
          e2.printStackTrace();
        }
      }
    }
    
  }
}