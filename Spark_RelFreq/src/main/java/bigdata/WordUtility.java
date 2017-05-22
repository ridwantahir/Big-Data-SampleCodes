package bigdata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


import org.apache.hadoop.io.Text;

import scala.Tuple2;

public class WordUtility 
{
	private static int index_count;
	public static Iterator<Tuple2<String, DistributionMap>> generateTuples(String line)
	{
		List<Tuple2<String, DistributionMap>> tuplesList=new ArrayList<>();
		List<String> allWords = Arrays.asList(line.split("\\s+"));
		index_count=0;
    	allWords.forEach((String word)->
    	{
    		word=word.trim();
    		if(index_count!=0)
    		{
	    		DistributionMap tempMap=new DistributionMap();
	    	    
	    	    WordUtility.getNeighbours(allWords, index_count).forEach(neighbour->
				{
					tempMap.addFrequency(neighbour);					
				});
	    	    tuplesList.add(new Tuple2<String, DistributionMap>(word,tempMap));
    		}
    	    index_count++;
    	    
    	});
    	return tuplesList.iterator();
	}
	public static List<String> getNeighbours(List<String> allWords, int index )
	{
		List<String> neighbours=new ArrayList<String>();
		
		for(int i=index+1;i<allWords.size();i++)
		{
				if(allWords.get(index).equals(allWords.get(i)))break;
				neighbours.add(allWords.get(i).trim());
		}
		return neighbours;
	}
}
