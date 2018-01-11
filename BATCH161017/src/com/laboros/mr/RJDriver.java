package com.laboros.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.laboros.mapper.CustMapper;
import com.laboros.mapper.TxnMapper;
import com.laboros.reducer.RJReducer;

//yarn jar RJDemo.jar com.laboros.mr.RJDriver custs txns RJOP
public class RJDriver extends Configured implements Tool {

	public static void main(String[] args) {

		// Total 3 steps
		// Validation
		if (args.length < 3) {
			System.out
					.println("Java Usage "
							+ RJDriver.class.getName()
							+ " [configuration] /path/to/hdfs/custs/file " 
							+" /path/to/hdfs/txns/file" 
							+" /path/to/hdfs/destination/dir");
			return;
		}
		// Loading Configuration
		Configuration conf = new Configuration(Boolean.TRUE);
		// Invoke ToolRunner.run

		try {
			int i = ToolRunner.run(conf, new RJDriver(), args);
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
		Job rjJob = Job.getInstance(conf, RJDriver.class.getName());
		
		//Step-3: Setting the classpath of the mapper class jar name 
		//on the datanode
		rjJob.setJarByClass(RJDriver.class);
		
		//step-4: Setting the input
		//step: 4-a setting the customer input
		final String hdfsCustInput = args[0];
		final Path hdfsCustInputPath = new Path(hdfsCustInput);
		MultipleInputs.addInputPath(rjJob, hdfsCustInputPath, TextInputFormat.class, CustMapper.class);
		
		//step: 4-b setting the txn input
		final String hdfsTxnInput = args[1];
		final Path hdfsTxnInputPath = new Path(hdfsTxnInput);
		MultipleInputs.addInputPath(rjJob, hdfsTxnInputPath, TextInputFormat.class, TxnMapper.class);
		//step-5: Setting the output
		
		final String hdfsOutput=args[2];
		final Path hdfsOutputPath = new Path(hdfsOutput);
		TextOutputFormat.setOutputPath(rjJob, hdfsOutputPath);
		rjJob.setOutputFormatClass(TextOutputFormat.class);
		
		
		//step-7: Setting the Reducer
		rjJob.setReducerClass(RJReducer.class);
		//step-8: Set the Mapper Output Key and Value classes
		
		
//		rjJob.setMapOutputKeyClass(Text.class);
//		rjJob.setMapOutputValueClass(IntWritable.class);
		
		//step-9: Set the Reducer Output Key and Value classes 
		
		
		rjJob.setOutputKeyClass(Text.class);
		rjJob.setOutputValueClass(Text.class);
		//Step: 10 : Trigger Method
		rjJob.waitForCompletion(Boolean.TRUE);
		return 0;
	}

}
