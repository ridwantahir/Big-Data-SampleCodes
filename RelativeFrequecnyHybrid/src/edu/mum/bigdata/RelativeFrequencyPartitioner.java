package edu.mum.bigdata;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class RelativeFrequencyPartitioner extends Partitioner<Pair,IntWritable> 
{

	@Override
	public int getPartition(Pair keyPair, IntWritable count, int numReduceTask) 
	{
		int start=keyPair.getWord_main().toString().charAt(0);
		//System.out.println("start "+start);
		//System.out.println("The paritioner out is : "+(start & Integer.MAX_VALUE)%numReduceTask);
		return (start & Integer.MAX_VALUE)%numReduceTask;
		
	}

}
