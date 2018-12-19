package com.maplecloudy.spider.protocol.httpmethod;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.jsoup.Jsoup;

import com.google.common.collect.Lists;

public class Test {

	public static void main(String[] args) throws Exception {
		
		while (true) {
			List<String> e = Lists.newArrayList();
			String eString = Jsoup.connect("http://maplecloudy.v4.dailiyun.com/query.txt?key='' or 1=1#' &word=&count=1000&rand=true&detail=true").ignoreContentType(true).get().toString().trim();
			System.out.println(eString);
			eString = eString.split("<body>")[1].split("</body>")[0].trim();
			String[] se = eString.split(" ");
			for (int i = 0; i < se.length; i++) {
				e.add(se[i].split(",")[0]);
			}
			ProxyWithEs.getInstance().proxyToEs(e);
			int s = new Random().nextInt(3)*60 *1000 + new Random().nextInt(6)*10 *1000;
			if (s < 90 * 1000) s = 9 * 10 *1000;
			Thread.sleep(s);
		}
		
	}
}
