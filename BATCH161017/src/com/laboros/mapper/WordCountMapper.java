package com.laboros.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	protected void setup(
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable>.Context arg0)
			throws java.io.IOException, InterruptedException {
	};

	@Override
	protected void map(
			LongWritable key,
			Text value,
			Context context)
			throws java.io.IOException, InterruptedException {
		//Business logic -1
		
		
	};

	protected void cleanup(
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable>.Context arg0)
			throws java.io.IOException, InterruptedException {
	};

}
