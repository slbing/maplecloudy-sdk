package com.maplecloudy.spider.crawl;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;

import com.maplecloudy.oozie.main.OozieMain;
import com.maplecloudy.oozie.main.OozieToolRunner;
import com.maplecloudy.spider.fetcher.FetcherSmart;
import com.maplecloudy.spider.parse.ParseSegment;
import com.maplecloudy.spider.util.SpiderConfiguration;
import com.maplecloudy.spider.util.SpiderJob;

public class Crawl extends OozieMain implements Tool {
	public static final Log LOG = LogFactory.getLog(Crawl.class);


	public Crawl() {
	}

	@Override
	public int run(String args[]) throws IOException, ClassNotFoundException, IllegalStateException, InterruptedException {
		// Crawl cr = new Crawl();
		JobConf job = new SpiderJob(this.getConf());

		Path dir = new Path("iphone");

		Path crawlDb = null;
		Path seg = null;
		int threads = job.getInt("fetcher.threads.fetch", 10);

		FileSystem fs = FileSystem.get(job);
		Injector injector = new Injector(this.getConf());
		GeneratorSmart generator = new GeneratorSmart(this.getConf());
		FetcherSmart fetcher = new FetcherSmart(this.getConf());
		CrawlDb crawlDbTool = new CrawlDb(this.getConf());
		ParseSegment parse = new ParseSegment(this.getConf());
		crawlDb = new Path(dir + "/crawldb");
		seg = new Path(dir + "/segments");
		injector.inject(crawlDb, new Path(dir, "iphone.seed"));
		try {
			while (true) {

				Path[] segments = null;
				segments = generator.generate(crawlDb, seg, 1, System.currentTimeMillis(), false);
				if (segments == null) {
					LOG.info("Stopping dute no more URLs to fetch.");
					break;
					// return;
				}
				for (Path segment : segments) {
					fetcher.fetch(segment, threads); // fetch it
					parse.parse(segment);
				}
				crawlDbTool.update(crawlDb, segments); // update
			}
			LOG.info("Crawl is done!\n");
			return 0;
			
		} catch (Exception e) {
			if (LOG.isFatalEnabled())

				LOG.fatal("in CrawlInfo main() Exception " + StringUtils.stringifyException(e) + "\n");
			return -1;
		}
		
		
	}

	/* Perform complete crawling and indexing given a set of root urls. */
	public static void main(String args[]) throws Exception {
		System.out.println(org.apache.commons.lang.StringUtils.join(args, " "));
		int res = OozieToolRunner.run(new Configuration(), new Crawl(), args);
		System.exit(res);

	}

}
