package edu.mum.bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/*
 * This class is a  container for relative frequencies= stripe... its something like
 * {(charry, 3),(banana, 8),(mango, 4)}. It is just a container... 
 */
public class DistributionMap implements Writable
{
	private MapWritable map=new MapWritable();
	public void clear()
	{
		this.map.clear();
	}
	public int getSize()
	{
		return map.size();
	}
	public  boolean isEmpty()
	{
		return map.size()==0;
	}
	/*
	 * This method performs element wise merging of Distribution maps. example
	 * (charry, 3),(banana, 8),(mango, 4)} merged with (charry, 4),(banana, 8),(orange, 4)} gives
	 * {(cherry,7),(banana,16),(mango,4), (orange, 4)}
	 */
	public void mergeWithOtherFreqMap(DistributionMap otherMap)
	{
		Set<Writable> curKeys=otherMap.map.keySet();
		for(Writable key:curKeys)
		{
			if(this.map.containsKey(key))
			{
				int old=((IntWritable)(this.map.get(key))).get();
				int to_add=((IntWritable)(otherMap.map.get(key))).get();
				map.put(key, new IntWritable(old+to_add));
			}
			else
			{				
				int to_add=((IntWritable)(otherMap.map.get(key))).get();
				map.put(key, new IntWritable(to_add));
			}
		}
	}
	/*
	 * This Method convertes co occurrance to relative frequencie.......
	 * it divided each count in the by the marginal.. it convertes it to percentage
	 */
	public DistributionMap computeOcurrenceprobability()
	{
		DistributionMap ProbabilityMap=new DistributionMap();
		
		//MapWritable ProbabilityMap=new MapWritable();
		int marginal=0;
		Set<Writable> keys=map.keySet();
		for(Writable t: keys)
		{
			marginal = marginal+((IntWritable)map.get(t)).get();
		}
		for(Writable t: keys)
		{
			int tempInt=(((IntWritable)map.get(t)).get()*100)/marginal;
			ProbabilityMap.map.put(t, new IntWritable(tempInt));
		}
		return ProbabilityMap;
		//return null;
	}
	/*
	 * This method adds pair to a stripe for example if this method is 
	 * called with (mango,6) on a stripe which is originally {(banana,4)} it
	 * will become {(banana,4),(mango,6)}
	 */
	public void addFrequency(Text t, int freq)
	{
		if(map.get(t)==null)
		{
			map.put(t, new IntWritable(freq));
		}
		else
		{
			IntWritable newvalue=new IntWritable(((IntWritable)(map.get(t))).get()+freq);
			map.put(t, newvalue);
		}
	}
	/*
	 * This method adds pair to a stripe for example if this method is 
	 * called with (mango) on a stripe which is originally {(banana,4)} it 
	 * will become {(banana,4),(mango,1)}
	 */
	public void addFrequency(Text t)
	{
		if(map.get(t)==null)
		{
			map.put(t, new IntWritable(1));
		}
		else
		{
			IntWritable newvalue=new IntWritable(((IntWritable)(map.get(t))).get()+1);
			map.put(t, newvalue);
		}
	}
	@Override
	public void readFields(DataInput in) throws IOException 
	{
		map.readFields(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException 
	{
		map.write(out);
		
	}

	@Override
	public String toString() 
	{
		String output="";
		Set<Writable> keys=map.keySet();
		output+="{";
		//System.out.print("{");
		int i=0;
		for(Writable t: keys)
		{
			Text item=(Text)t;
			IntWritable count=(IntWritable)map.get(t);
			if(i==0)
			{
				output+="("+item.toString()+", "+count+")";
			}
			else
			{
				output+=", ("+item.toString()+", "+count+")";
			}
			i++;
		}
		output+="}";
		return output;
	}
	

}
