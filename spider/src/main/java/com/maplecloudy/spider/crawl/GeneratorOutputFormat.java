package com.maplecloudy.spider.crawl;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.maplecloudy.avro.mapreduce.output.AvroPairOutputFormat;
import com.maplecloudy.avro.mapreduce.output.MultipleOutputs;
import com.maplecloudy.spider.crawl.GeneratorSmart.SelectorEntry;


public class GeneratorOutputFormat extends
		FileOutputFormat<Float, SelectorEntry> {

	@Override
	public RecordWriter<Float, SelectorEntry> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		
		final MultipleOutputs mos = new MultipleOutputs(job);
//		mos.addNamedOutput("fetchlist", AvroPairOutputFormat.class, Float.class, SelectorEntry.class);
		
		return new RecordWriter<Float, SelectorEntry>(){

			@Override
			public void write(Float key, SelectorEntry value) throws IOException,
					InterruptedException {
				
				mos.write(AvroPairOutputFormat.class, generateFileNameForKeyValue(key, value),key, value);
			}

			@Override
			public void close(TaskAttemptContext context) throws IOException,
					InterruptedException {
				mos.close();
				
			}
			
			String generateFileNameForKeyValue(Float key, SelectorEntry value) {
				return "fetchlist-" + value.segnum;
			}
		};
	}
}
