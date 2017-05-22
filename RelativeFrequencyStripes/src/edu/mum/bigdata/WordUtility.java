package edu.mum.bigdata;

import java.util.ArrayList;
import java.util.List;


import org.apache.hadoop.io.Text;

public class WordUtility 
{
	public static List<Text> getNeighbours(List<String> allWords, int index )
	{
		List<Text> neighbours=new ArrayList<Text>();
		
		for(int i=index+1;i<allWords.size();i++)
		{
				if(allWords.get(index).equals(allWords.get(i)))break;
				neighbours.add(new Text(allWords.get(i).trim()));
		}
		return neighbours;
	}
}
