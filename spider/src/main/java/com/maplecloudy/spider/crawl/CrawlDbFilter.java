package com.maplecloudy.spider.crawl;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;

import com.maplecloudy.spider.net.BasicURLNormalizer;
import com.maplecloudy.spider.net.URLNormalizer;

/**
 * This class provides a way to separate the URL normalization and filtering
 * steps from the rest of CrawlDb manipulation code.
 * 
 * @author Andrzej Bialecki
 */
public class CrawlDbFilter extends Mapper<String, CrawlDatum, String, CrawlDatum> {
	private URLNormalizer		normalizers;

	public static final Log	LOG	= LogFactory.getLog(CrawlDbFilter.class);

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		normalizers = new BasicURLNormalizer();
	}

	@Override
	protected void map(String key, CrawlDatum value, Context context)
			throws IOException, InterruptedException {
		String url = key.toString();
		try {
			url = normalizers.normalize(url); // normalize the url
		} catch (Exception e) {
			LOG.warn("Skipping " + url + ":" + e);
			url = null;
		}

		if (url != null) { // if it passes
			context.write(url, value);
		}
	}
}
