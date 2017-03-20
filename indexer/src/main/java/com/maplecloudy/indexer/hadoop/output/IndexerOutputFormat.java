package com.maplecloudy.indexer.hadoop.output;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.maplecloudy.indexer.writer.GeneralWriter;
import com.maplecloudy.indexer.writer.IndexWriter;
import com.maplecloudy.indexer.writer.SolrWriter;

public  class IndexerOutputFormat<K,V> extends FileOutputFormat<K, V> {

	@Override
	public RecordWriter<K, V> getRecordWriter(
			TaskAttemptContext job) {
		final IndexWriter writer;
		try {
			writer = new GeneralWriter();

			writer.open(job, getDefaultWorkFile(job, ""));

			return new RecordWriter<K, V>() {

				@Override
				public void write(K key, V value)
						throws IOException, InterruptedException {
					try {
						writer.write(value);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

				@Override
				public void close(TaskAttemptContext context)
						throws IOException, InterruptedException {
					try {
						writer.close();
					} catch (Exception e) {
						e.printStackTrace();
						throw new IOException(e);
					}
				}
			};
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
 
	public void checkOutputSpecs(JobContext job)
			throws IOException {
		Path outDir = getOutputPath(job);
		if (outDir == null) {
			throw new InvalidJobConfException("Output directory not set.");
		}
	}

	public static void setWriterClass(Job job,
			Class<? extends IndexWriter> clazz) {
		job.getConfiguration().setClass(SolrWriter.SOLR_WRITER_NAME,
				clazz,IndexWriter.class);
	}
}
