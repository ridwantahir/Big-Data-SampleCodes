package edu.mum.bigdata;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class RelativeFrequencyPartitioner extends Partitioner<Text,DistributionMap> 
{

	@Override
	public int getPartition(Text keyWord, DistributionMap map, int numReduceTask) 
	{
		int start=keyWord.toString().charAt(0);
		//System.out.println("start "+start);
		//System.out.println("The paritioner out is : "+(start & Integer.MAX_VALUE)%numReduceTask);
		return (start & Integer.MAX_VALUE)%numReduceTask;
	}

}
