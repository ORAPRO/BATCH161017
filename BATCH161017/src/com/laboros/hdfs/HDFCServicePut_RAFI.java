/**
 * 
 */
package com.laboros.hdfs;

import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author dell
 *
 */
public class HDFCServicePut_RAFI extends Configured implements Tool {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		/**
		 * Hadoop FS -put wordCount.txt /usr/training
		 * 1. Loading the Configuration
		 * 2. Setting the Classpath
		 * 3. Identify the Command
		 * 4. Identigy the Java class.
		 * 
		 *  Data set = meta data +data
		 *  Creating meta data
		 *  Step1 = Creating the EMPTY File (it should be in terms of File System Object)
		 *  Step2 = Read the Filename from the Command Line args 
		 *  Step3 = Get the destination directory ( HDFS understand as URI)
		 *  Step4 = Name Node perform 2 validations 
		 *  a) file exist or not ( else condition FileAlreadyExistsException) 
		 *  b) Write permission 
		 *  else it will create an empty File.
		 */
		
		//First 
		// Loading Configuration
				Configuration conf = new Configuration(Boolean.TRUE);
				conf.set("fs.defaultFS", "hdfs://localhost:8020");
				
				try {
					int i = ToolRunner.run(conf, new HDFCServicePut_RAFI(), args);
					if (i==0)
					{
						System.out.println("Susccess");
					}
							
				}catch(Exception e){
					System.out.println("FAILED");
					e.printStackTrace();
				}
		
		
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
	
		/**
		 *  Data set = meta data +data
		 *  Creating meta data
		 *  Step1 = Creating the EMPTY File (it should be in terms of File System Object)
		 *  Step2 = Read the Filename from the Command Line args 
		 *  Step3 = Get the destination directory and create the Path ( HDFS understand as URI)
		 *  Step4 = Name Node perform 2 validations 
		 *  a) file exist or not ( else condition FileAlreadyExistsException) 
		 *  b) Write permission 		
		 */
		
		
		final String inputFileName = args[0];
		System.out.println("inputFileName -> " + inputFileName);
		
		final String hdfsDestinationDir=args[1];
		System.out.println("HDFS Destination dir:"+hdfsDestinationDir);
		
		//Convert into URI because hdfs understand URI
		final Path hdfsDestPathWithFileName = new Path(hdfsDestinationDir, inputFileName);
		
		//Create the FileSystem
		
		Configuration conf = super.getConf();
			
		FileSystem hdfs = FileSystem.get(conf);
		
		FSDataOutputStream fsdos=hdfs.create(hdfsDestPathWithFileName);
		
		/**Step:2 
		 * 
		 * Adding Data
		 * 
		 * Splitting Data into blocks
		 * Identify the Datanode for data block -- DataStreamer
		 * Writing datablock to DN
		 * Replication
		 * Sync with NN
		 * Handling Failures.
		 */
		
		InputStream is = new FileInputStream(inputFileName);
		IOUtils.copyBytes(is, fsdos, conf, Boolean.TRUE);
		
		return 0;
	}

}
