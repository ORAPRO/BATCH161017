package com.laboros.hdfs;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//java -cp /path/to/jars com.laboros.hdfs.HDFSServicePut [configurations] WordCount.txt /user/trainings
public class HDFSServicePut extends Configured implements Tool {

	public static void main(String[] args) {

		// Total 3 steps
		// Validation
		if (args.length < 2) {
			System.out.println("Java Usage "+HDFSServicePut.class.getName()+" [configuration] /path/to/edge/node/local/file /path/to/hdfs/destination/dir");
			return;
		}
		// Loading Configuration
		Configuration conf = new Configuration(Boolean.TRUE);
//		print(conf);
		
		// Invoke ToolRunner.run
		
		try {
			int i=ToolRunner.run(conf, new HDFSServicePut(), args);
			if(i==0)
			{
				System.out.println("SUCCESS");
			}
		} catch (Exception e) {
			System.out.println("FAILED");
			e.printStackTrace();
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		
		

		return 0;
	}
	
//	private static void print(Configuration conf)
//	{
//		for (Map.Entry<String, String> entry : conf) {
//			System.out.println(entry.getKey()+"===="+entry.getValue());
//		}
//	}

}
