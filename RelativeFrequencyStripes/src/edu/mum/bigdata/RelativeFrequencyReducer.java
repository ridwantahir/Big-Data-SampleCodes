package edu.mum.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RelativeFrequencyReducer extends Reducer< Text,DistributionMap,Text,DistributionMap>
{
	@Override
	public void reduce(Text keyword,Iterable<DistributionMap> mapList, Context context) throws IOException, InterruptedException
	{
		DistributionMap intermidiateMap=new DistributionMap();
		for(DistributionMap cuMap: mapList)
		{
			intermidiateMap.mergeWithOtherFreqMap(cuMap);
		}
		DistributionMap outputMap= intermidiateMap.computeOcurrenceprobability();
		context.write(keyword, outputMap);		
	}

}
