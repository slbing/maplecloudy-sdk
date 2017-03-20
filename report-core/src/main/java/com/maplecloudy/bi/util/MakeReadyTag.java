package com.maplecloudy.bi.util;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

import com.maplecloudy.oozie.main.OozieMain;
import com.maplecloudy.oozie.main.OozieToolRunner;

public class MakeReadyTag  extends OozieMain implements Tool{
	
	private String sPath = null;
	public static final String READY_TAG = ".ready";
	
	@Override
	public int run(String[] args) throws Exception {
		if (args == null || args.length < 1) {
			System.out.println("Usage: <path>");
			return -1;
		}
		
		sPath = args[0];
		makePath();
		
		return 0;
	}
	
	protected void makePath() throws IOException{
		Path path = new Path(sPath,READY_TAG);
        FileSystem hdfs = path.getFileSystem(this.getConf());
        if (!hdfs.exists(path)) hdfs.createNewFile(path);
	}

	public static void main(String[] args) throws Exception {
		int res = OozieToolRunner.run(ReportConfiguration.create(),
				new MakeReadyTag(), args);
		System.exit(res);
	}

}
