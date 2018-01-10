package com.laboros.mr.partitioner;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, IntWritable> 
{

	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) 
	{
		if(StringUtils.equalsIgnoreCase(key.toString(), "HADOOP"))
		{
			return 0;
		}
		if(StringUtils.equalsIgnoreCase(key.toString(), "DATA"))
		{
			return 1;
		}
		return 2;
	}

}
