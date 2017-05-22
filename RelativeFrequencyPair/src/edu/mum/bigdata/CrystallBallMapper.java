package edu.mum.bigdata;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

public class CrystallBallMapper extends Mapper<LongWritable,Text,Pair,IntWritable> {

	Map<Pair,IntWritable> eventMap;
	
	// /home/cloudera/workspace/CrystallBall/input
	@Override
	protected void setup(
			Mapper<LongWritable, Text, Pair, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		 eventMap=new HashMap<>();		
	}
	
	@Override
	protected void map(LongWritable key, Text record,

			Mapper<LongWritable, Text, Pair, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		 String currentEvent=null;
		
		 String[] events;//=  record.toString().split(" ");
		 StringTokenizer itr = new StringTokenizer(record.toString());
			List<String> allWords=new ArrayList<String>();
			while (itr.hasMoreTokens()) 
			{
				allWords.add(itr.nextToken().trim());
			}
		 events=allWords.toArray(new String[allWords.size()]);
		
	     for(int i=1;i<events.length;i++){
	    	 currentEvent=events[i];
	    	
	    	 
	    	 for(int j=i+1;j<events.length;j++){
	    		 
	    		 
	    		 
	    		 	if(!events[j].equals(currentEvent)){
	    		 		Pair p=new Pair(currentEvent,events[j]);
	    		 		 Pair starPair=new Pair(currentEvent,"*");
	    			 		if(eventMap.containsKey(starPair))
	    			 		{
	    			 			int c=eventMap.get(starPair).get()+1;
	    			 			IntWritable iw=new IntWritable(c);
	    			 			if(iw!=null){
	    			 			eventMap.put(starPair,iw);
	    			 			}
	    			 		}
	    			 		else
	    			 		{
	    			 			eventMap.put(starPair,new IntWritable(1));
	    			 		}
	    		 		
	    		 	
	    		 		if(eventMap.containsKey(p))
	    		 		{
	    		 			int c=eventMap.get(p).get()+1;
	    		 			IntWritable iw=new IntWritable(c);
	    		 			if(iw!=null){
	    		 			eventMap.put(p,iw);
	    		 			}
	    		 		}
	    		 		else
	    		 		{
	    		 				eventMap.put(p,new IntWritable(1));
	    		 		}
	    		 			
	    		 		}
	    		 	else
	    		 	{
	    		 		break;
	    		 	}
	    		 	
	    		 
	    		 }
	    		 	
	    			
	    	 }
	    	 
	    	
	     }
		
	@Override
	protected void cleanup(
			Mapper<LongWritable, Text, Pair, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		System.out.println("Clean up starting");
		for(Entry<Pair, IntWritable> e:eventMap.entrySet()){
		
			context.write(e.getKey(), e.getValue());
			
		
			System.out.println(e.getKey()+ ","+ e.getValue());
		}
		
		
		
		
		
	}
	
	
	}
	
	
	/*
	private boolean hasEventPair(Pair p){
		
		Iterator it=eventMap.entrySet().iterator();
		while(it.hasNext())
		{
			Map.Entry pair=(Map.Entry)it.next();
		    Pair key=(Pair)	pair.getKey();
		    if((key.getEventID()==p.getEventID() )&& (key.getCoEventID()==p.getCoEventID())){
		    	
		    	oneeventMapiw
		    
		    		
		    }
		}
	}
	*/
	


