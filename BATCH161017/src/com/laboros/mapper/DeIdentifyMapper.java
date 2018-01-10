package com.laboros.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DeIdentifyMapper extends
		Mapper<LongWritable, Text, Text, NullWritable> {
	
	final int[] deIdentifyCols = {2,3,5,7};

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		// key -- 0
		// value --
		// 11111,bbb1,12/10/1950,1234567890,bbb1@xxx.com,
		// 1111111111,M,Diabetes,78
		
		final String iLine = value.toString();
		
		final StringBuffer output = new StringBuffer();
		
		if(StringUtils.isNotEmpty(iLine))
		{
			final String[] columns = StringUtils.splitPreserveAllTokens(iLine, ",");
			
			for (int i = 0; i < columns.length; i++) {
				if(checkColEncrypt(i)){
					output.append(encrypt(columns[i]));
				}else{
					output.append(columns[i]);
				}
				output.append(",");
			}
			
			context.write(new Text(output.toString()), NullWritable.get());
			
		}
		
		

	}

	private String encrypt(String string) {
		//Build the encryption
		return "XXXXX-XXXX";
	}

	private boolean checkColEncrypt(int i) {
		for (int j = 0; j < deIdentifyCols.length; j++) {
			if((i+1)==deIdentifyCols[j]){
				return Boolean.TRUE;
			}
			
		}
		return Boolean.FALSE;
	};
	
	
}
