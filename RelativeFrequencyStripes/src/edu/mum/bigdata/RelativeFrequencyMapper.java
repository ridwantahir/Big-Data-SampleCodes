package edu.mum.bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class RelativeFrequencyMapper extends Mapper<LongWritable, Text,Text, DistributionMap > 
{
	HashMap<Text,DistributionMap>  AssocMap;
	public static int count=1000;
	/*
	 This gives an option to flush the memory in case the associative array  gets too large 
	*/
	public void flush(Context context) throws IOException, InterruptedException
	{

		Set<Text> keys=AssocMap.keySet();
		for(Text t: keys)
		{
			if(!AssocMap.get(t).isEmpty())
			{
				context.write(t, AssocMap.get(t));
				System.out.println(t+"   "+AssocMap.get(t).toString());
			}
		}
		AssocMap.clear();
	}
	@Override
	public void setup(Context context)
	{
		 AssocMap= new HashMap<>();
		 
	}
	@Override
	public  void map(LongWritable key, Text doc, Context context) throws IOException, InterruptedException
	{
		StringTokenizer itr = new StringTokenizer(doc.toString());
		List<String> allWords=new ArrayList<String>();
		while (itr.hasMoreTokens()) 
		{
			allWords.add(itr.nextToken().trim());
		}
		allWords.remove(0);
		int index_count=0;
		for(String main_word:allWords)
		{
			List<Text> neighbours=WordUtility.getNeighbours(allWords, index_count);
			DistributionMap tempMap=new DistributionMap();			
			for(Text neighbour: neighbours)
			{
				tempMap.addFrequency(neighbour);
			}
			index_count++;
			//System.out.println(AssocMap.get(main_word));
			//System.out.println(main_word);
			if(AssocMap.get(new Text(main_word))==null)
			{
				AssocMap.put(new Text(main_word), tempMap);
			}
			else
			{
				AssocMap.get(new Text(main_word)).mergeWithOtherFreqMap(tempMap);
			}
			if(AssocMap.size()>=count)// if the memory is too large, flush it
			{
				this.flush(context);
			}
		}
	}
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException
	{
		this.flush(context);
	}
	
}
