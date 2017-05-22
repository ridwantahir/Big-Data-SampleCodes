package edu.mum.bigdata;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RelativeFrequencyReducer extends Reducer<Pair,IntWritable,Text,DistributionMap>
{
	DistributionMap DistMap;
	Text currentTerm;
	
	void flush(Context context) throws IOException, InterruptedException
	{
		context.write(currentTerm, DistMap.computeOcurrenceprobability());
		DistMap.clear();	
	}
	@Override
	public void setup(Context context)
	{
		DistMap=new DistributionMap();
		currentTerm=null;
	}
	
	@Override
	public void reduce(Pair keyPair, Iterable<IntWritable> countList, Context context) throws IOException, InterruptedException
	{
		//System.out.println(keyPair);
		int count_sum=0;
		if(currentTerm!=null && !currentTerm.equals(keyPair.getWord_main()))
		{
			this.flush(context);		
		}
		currentTerm=new Text(keyPair.getWord_main().toString());
		for(IntWritable count:countList)
		{
			System.out.println(keyPair.toString() + count+" ");
			count_sum=count_sum+count.get();
		}
		Text co_occuring=new Text(keyPair.getWord_co().toString());
		//System.out.println(count_sum);
		DistMap.addFrequency(co_occuring,count_sum);
		//System.out.println();
		//System.out.println("item:  "+currentTerm+"  "+co_occuring);
		//System.out.println(DistMap);
	}
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException
	{
		this.flush(context);
	}
}
