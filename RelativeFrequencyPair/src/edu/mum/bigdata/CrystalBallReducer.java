package edu.mum.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CrystalBallReducer extends Reducer<Pair,IntWritable,Pair,IntWritable> {
	
	int marginal=0;
	

	public void reduce(Pair key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
		System.out.println(key);
		
		if(key.getCoEventID().equals("*"))
		{
			marginal=0;
			for(IntWritable value:values){
				marginal+=value.get();
			}
		}
		else
		{
			int sum=0;
			for(IntWritable value:values){
				sum+=value.get();
			}
			context.write(key,new IntWritable((sum*100)/marginal));
			//context.write(key,new IntWritable(sum));
			
		}
		
	}
}