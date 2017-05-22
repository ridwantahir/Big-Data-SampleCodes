package edu.mum.bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RelativeFrequencyMapper extends Mapper<LongWritable, Text, Pair,IntWritable >
{
	
	HashMap<Pair, IntWritable> AssocMap;
	public static int count=1000;
	/*
	 This gives an option to flush the memory in case the associative array  gets too large 
	*/
	
	void flush(Context context) throws IOException, InterruptedException
	{
		Set<Pair>keys=AssocMap.keySet();
		for(Pair p: keys)			
		{
			context.write(p, AssocMap.get(p));
			//System.out.println(p+", "+AssocMap.get(p));
		}
		AssocMap.clear();
	}
	@Override
	public void setup(Context context)
	{
		AssocMap= new HashMap<>();
	}
	@Override
	public void map(LongWritable key, Text doc, Context context ) throws IOException, InterruptedException
	{
		//System.out.println("begiining of mapper");
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
			for(Text neighbour: neighbours)
			{
				Pair p=new Pair(new Text(main_word),new Text(neighbour));
				if(AssocMap.get(p)==null)
				{
					AssocMap.put(p, new IntWritable(1));
				}
				else
				{
					int new_one=AssocMap.get(p).get()+1;
					AssocMap.put(p, new IntWritable(new_one));
				}				
			}
			index_count++;
		}
		if(AssocMap.size()>=count)// if the associative array size is too large, flush it
		{
			this.flush(context);
		}
		//System.out.println("end of mapper");
	}
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException
	{
		//System.out.println("begiining of mapper clean up");
		this.flush(context);
		//System.out.println("end of mapper cleanup");
	}
	/*public static void main(String[] args) throws IOException, InterruptedException
	{
		RelativeFrequencyMapper maper=new RelativeFrequencyMapper();
		maper.setup(null);
		maper.map(null, new Text("cherry mango olive cherry"), null);
		maper.map(null, new Text("mango olive banana cherry"), null);
		maper.map(null, new Text("cherry banana mango banana"), null);
		maper.map(null, new Text("mango cherry banana olive"), null);
		maper.map(null, new Text("cherry mango olive banana"), null);
		maper.map(null, new Text("mango olive banana olive"), null);
		maper.cleanup(null);
	}*/
	

}
