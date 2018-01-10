package com.laboros.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DeIdentifyDriver extends Configured implements Tool {

	public static void main(String[] args) {

		// Total 3 steps
		// Validation
		if (args.length < 2) {
			System.out
					.println("Java Usage "
							+ DeIdentifyDriver.class.getName()
							+ " [configuration] /path/to/hdfs/file " +
							"/path/to/hdfs/destination/dir");
			return;
		}
		// Loading Configuration
		Configuration conf = new Configuration(Boolean.TRUE);
		// Invoke ToolRunner.run

		try {
			int i = ToolRunner.run(conf, new DeIdentifyDriver(), args);
			if (i == 0) {
				System.out.println("SUCCESS");
			}
		} catch (Exception e) {
			System.out.println("FAILED");
			e.printStackTrace();
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		//Total 10 steps
		
		//step -1 : Getting the configuration
		//This configuration will set the parameters in the ToolRunner.run
		//Getting the configuration from super
		
		Configuration conf = super.getConf(); 
		
		//step-2: Create Job Instance
		Job deIdentifyJob = Job.getInstance(conf, DeIdentifyDriver.class.getName());
		
		//Step-3: Setting the classpath of the mapper class jar name 
		//on the datanode
		deIdentifyJob.setJarByClass(DeIdentifyDriver.class);
		
		//step-4: Setting the input
		final String hdfsInput = args[0];
		final Path hdfsInputPath = new Path(hdfsInput);
		TextInputFormat.addInputPath(deIdentifyJob, hdfsInputPath);
		deIdentifyJob.setInputFormatClass(TextInputFormat.class);
		
		//step-5: Setting the output
		
		final String hdfsOutput=args[1];
		final Path hdfsOutputPath = new Path(hdfsOutput);
		TextOutputFormat.setOutputPath(deIdentifyJob, hdfsOutputPath);
		deIdentifyJob.setOutputFormatClass(TextOutputFormat.class);
		
		//Step-6: Setting the Mapper
		
//		deIdentifyJob.setMapperClass(DeIdentifyMapper.class);
		
		//step-7: Setting the Reducer
//		deIdentifyJob.setReducerClass(DeIdentifyReducer.class);
		//step-8: Set the Mapper Output Key and Value classes
		deIdentifyJob.setNumReduceTasks(0);
//		deIdentifyJob.setMapOutputKeyClass(Text.class);
//		deIdentifyJob.setMapOutputValueClass(NullWritable.class);
		
		//step-9: Set the Reducer Output Key and Value classes 
		
//		deIdentifyJob.setOutputKeyClass(Text.class);
//		deIdentifyJob.setOutputValueClass(IntWritable.class);
		//Step: 10 : Trigger Method
		deIdentifyJob.waitForCompletion(Boolean.TRUE);
		return 0;
	}

}
