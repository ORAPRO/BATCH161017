package com.laboros.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TxnMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(
			LongWritable key,
			Text value,
			Context context)
			throws java.io.IOException, InterruptedException 
			{
		//key -- 0
		//value -- 00000000,06-26-2011,4007024,
		//040.33,Exercise & Fitness,
		//Cardio Machine Accessories,Clarksville,Tennessee,credit
		
		final String iLine = value.toString();
		if(StringUtils.isNotEmpty(iLine))
		{
			final String[] columns = StringUtils.splitPreserveAllTokens(iLine, ",");
			context.write(new Text(columns[2]), new Text("TXNS\t"+columns[3]));
		}

			};
}








