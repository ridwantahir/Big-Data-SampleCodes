package edu.mum.bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HadoopDriver extends Configured implements Tool
{
	public static void main(String [] args) throws Exception
	{
		HadoopDriver driver =new HadoopDriver();
		int res=ToolRunner.run(driver, args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws  IOException, ClassNotFoundException, InterruptedException 
	{
		int NoRedTask =0;
		try
		{
			NoRedTask=Integer.parseInt(args[2]);
		}
		catch(Exception e)
		{
			NoRedTask=2;
		}

		 // System.out.println("left mapper");
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Frequency Occurrence");	    
	    job.setJarByClass(HadoopDriver.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(RelativeFrequencyMapper.class);
	    //job.setCombinerClass(InvertedIndexerReducer.class);
	    job.setReducerClass(RelativeFrequencyReducer.class);
	    job.setPartitionerClass(RelativeFrequencyPartitioner.class);
	    
	    
	    job.setMapOutputKeyClass(Pair.class);
	    job.setMapOutputValueClass(IntWritable.class);	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DistributionMap.class);  
	    
	    job.setNumReduceTasks(NoRedTask);
	    //System.out.println("not yet mapper");
	    return job.waitForCompletion(true) ? 0 : 1;
	}
}
