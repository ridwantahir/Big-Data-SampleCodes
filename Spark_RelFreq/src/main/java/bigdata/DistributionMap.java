package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;



/*
 * This class is a  container for relative frequencies= stripe... its something like
 * {(charry, 3),(banana, 8),(mango, 4)}. It is just a container... 
 */
public class DistributionMap implements Serializable
{
	private static final long serialVersionUID = 15931984L;
	private HashMap<String, Integer> map=new HashMap<>();
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
	public static DistributionMap mergeFreqMap(DistributionMap firstMap, DistributionMap otherMap)
	{
			DistributionMap result=new DistributionMap();
			Set<String>fkeys=firstMap.map.keySet();
			fkeys.forEach(key->result.map.put(key, firstMap.map.get(key)));
			Set<String> curKeys=otherMap.map.keySet();
			curKeys.forEach(key->
			{
				if(result.map.containsKey(key)) 
					result.map.put(key,result.map.get(key) +otherMap.map.get(key));
				else
					result.map.put(key, otherMap.map.get(key));
			});
			return result;			
	}
	public void mergeWithOtherFreqMap(DistributionMap otherMap)
	{
		Set<String> curKeys=otherMap.map.keySet();
		for(String key:curKeys)
		{
			if(this.map.containsKey(key))
			{
				int old=this.map.get(key);
				int to_add=otherMap.map.get(key);
				map.put(key,  old +to_add);
			}
			else
			{				
				int to_add=otherMap.map.get(key);
				map.put(key, to_add);
			}
		}
	}
	/*
	 * This Method convertes co occurrance to relative frequencie.......
	 * it divided each count in the by the marginal.. it convertes it to percentage
	 */
	public static DistributionMap computeOcurrenceprobability(DistributionMap other)
	{
		DistributionMap ProbabilityMap=new DistributionMap();
		
		//MapWritable ProbabilityMap=new MapWritable();
		int marginal=0;
		Set<String> keys=other.map.keySet();
		for(String t: keys)
		{
			marginal = marginal+other.map.get(t);
		}
		for(String t: keys)
		{
			int tempInt=(other.map.get(t)*100)/marginal;
			ProbabilityMap.map.put(t, tempInt);
		}
		return ProbabilityMap;
		//return null;
	}
	/*
	 * This method adds pair to a stripe for example if this method is 
	 * called with (mango,6) on a stripe which is originally {(banana,4)} it
	 * will become {(banana,4),(mango,6)}
	 */
	public void addFrequency(String t, int freq)
	{
		if(map.get(t)==null)
		{
			map.put(t, freq);
		}
		else
		{
			int newvalue=map.get(t)+freq;
			map.put(t, newvalue);
		}
	}
	/*
	 * This method adds pair to a stripe for example if this method is 
	 * called with (mango) on a stripe which is originally {(banana,4)} it 
	 * will become {(banana,4),(mango,1)}
	 */
	public void addFrequency(String t)
	{
		if(map.get(t)==null)
		{
			map.put(t, 1);
		}
		else
		{
			int newvalue=map.get(t)+1;
			map.put(t, newvalue);
		}
	}
	

	@Override
	public String toString() 
	{
		String output="";
		Set<String> keys=map.keySet();
		output+="{";
		//System.out.print("{");
		int i=0;
		for(String t: keys)
		{
			String item=t;
			int count=map.get(t);
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
