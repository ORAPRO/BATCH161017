package com.laboros.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCoutReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	protected void setup(
			org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable>.Context arg0)
			throws java.io.IOException, InterruptedException {
	};

	@Override
	protected void reduce(
			Text key,
			java.lang.Iterable<IntWritable> values,
			Context context)
			throws java.io.IOException, InterruptedException {
	};

	protected void cleanup(
			org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable>.Context arg0)
			throws java.io.IOException, InterruptedException {
	};

}
