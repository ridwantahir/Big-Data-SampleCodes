package edu.mum.bigdata;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CrystalBallPartitioner extends Partitioner<Pair,IntWritable> {

	@Override
	public int getPartition(Pair key, IntWritable value, int numReduceTasks) 
	{
		int start=key.getEventID().charAt(0);
		return (start & Integer.MAX_VALUE)% numReduceTasks;
	}

}
