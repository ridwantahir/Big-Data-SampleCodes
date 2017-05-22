package edu.mum.bigdata;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CrystalBallMain {
	 public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "CrystalBall Mapper");
		    
		    job.setJarByClass(CrystalBallMain.class);
		    
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    
		    
		   
		  
		    
		    
		    job.setMapperClass(CrystallBallMapper.class);
		  /*  job.setCombinerClass(WordCountReducer.class);*/
		    job.setPartitionerClass(CrystalBallPartitioner.class);
		    job.setReducerClass(CrystalBallReducer.class);
		    job.setNumReduceTasks(3);
		    job.setMapOutputKeyClass(Pair.class);
		    job.setMapOutputValueClass(IntWritable.class);
		    
		    job.setOutputKeyClass(Pair.class);
		    job.setOutputValueClass(IntWritable.class);    
		    
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		    
		    
		    
		    // /home/cloudera/workspace/CrystallBall/input
		    
		  }

}
