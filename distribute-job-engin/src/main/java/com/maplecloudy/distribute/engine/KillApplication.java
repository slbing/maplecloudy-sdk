package com.maplecloudy.distribute.engine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.maplecloudy.yarn.rpc.ClientRpc;

public class KillApplication {

	Configuration conf = new YarnConfiguration();
	
	public static void main(String[] args) throws Exception {
		
		if(args.length < 1)
			System.out.println("please give a application id to kill");
		String id = args[0];
		//application_1489135095681_0017
		String[] ids = id.split("_");
		ApplicationId appid = ApplicationId.newInstance(Long.parseLong(ids[1]), Integer.parseInt(ids[2])); 
		// Create yarnClient
		YarnConfiguration conf = new YarnConfiguration();
		YarnClient yarnClient = ClientRpc.getYarnClient(conf);
		yarnClient.killApplication(appid);
		
		
	}
}
