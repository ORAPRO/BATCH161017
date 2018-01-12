package com.laboros.reducer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RJReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(
			Text key,
			java.lang.Iterable<Text> values,
			Context context)
			throws java.io.IOException, InterruptedException {
		//key -- 4007024
		//values -- { 193.63, Kristina, 192,234,235}
		
		String name="";
		int count=0;
		double sum =0;
		for (Text ds_input : values) {
			final String[] tokens = StringUtils.splitPreserveAllTokens(ds_input.toString(), "\t");
			if(StringUtils.equalsIgnoreCase(tokens[0], "TXNS"))
			{
				sum = sum+Double.parseDouble(tokens[1]);
				count++;
			}else{
				name = tokens[1];
			}
			
		}
		if(StringUtils.isNotEmpty(name))
		{
		context.write(new Text(name), new Text(sum+"\t"+count));
		}	
	};
}
