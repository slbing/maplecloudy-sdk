package com.maplecloudy.bi.util.todb;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import com.maplecloudy.avro.io.UnionData;
import com.maplecloudy.bi.ReportConstants;

public class ReportToDbC {
	static int lAlgRecordCount = 0;

	/* MapReduce1将以sid为key的CPI数据集转换为以uid为key的单用户信息 */
	public static class M<K, V> extends Mapper<K, V, UnionData, UnionData>
	{
		TaskAttemptContext taskContext = null;
		ReportDbOutputFormat rdf = null;
		RecordWriter<Object, Object> writer = null;
		@Override
		protected void setup(Context context
				) throws IOException, InterruptedException {
			taskContext = new TaskAttemptContextImpl(context.getConfiguration(), new TaskAttemptID());
			rdf = new ReportDbOutputFormat(context.getConfiguration().get(ReportConstants.REPORT_TABLE_NAME));
			writer = rdf.getRecordWriter(taskContext);
			ToDbUtils.numRecordErr = 0L;
		}
		@Override
		protected void map(K key, V value, Context context) throws IOException, InterruptedException
		{
			Object rk = key instanceof UnionData ? ((UnionData) key).datum : key;
			writer.write(rk, value);
			lAlgRecordCount++;

		}
		@Override
		protected void cleanup(Context context
				) throws IOException, InterruptedException {
			/*
			 * AsyncSqlRecordWriter.close原来在mapreduce框架里面调用, 这里要显示调用: 1
			 * sql语句每100条批处理一次, close里面完成剩余的sql语句执行 2 close里面会阻塞等待sql执行线程结束才退出,
			 * 如果不调用主线程会退出, 进程退出
			 */
			writer.close(taskContext);

			System.out.println("runReport: write " + (lAlgRecordCount - ToDbUtils.numRecordErr) +"/" + lAlgRecordCount + 
					"(sucess/total) records to DB at time " + (new Date()).toString());
			System.out.println("Error data rows: " + ToDbUtils.numRecordErr +
					((ToDbUtils.numRecordErr==0)?"; Congratulations！":"; See details in log!"));
		}
	}

}
