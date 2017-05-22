package bigdata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.function.Function;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.spark_project.guava.collect.Lists;

import scala.Tuple2;


public class App 
{
	public static void computeRelativeFrequency(String inputfile, String outputdirectory)
	{
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Relativ Frequency");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile( inputfile,4 );
        JavaPairRDD<String,DistributionMap> tuplesColection=lines.flatMapToPair(WordUtility::generateTuples);
        JavaPairRDD<String,DistributionMap> reducedTuplesCollection=tuplesColection.reduceByKey(DistributionMap::mergeFreqMap,2);       
        JavaPairRDD<String,DistributionMap> relativeFreqCollection=reducedTuplesCollection.
        		mapToPair(tuple->new Tuple2<String,DistributionMap>(tuple._1,DistributionMap.computeOcurrenceprobability(tuple._2)));
        		
        relativeFreqCollection.saveAsTextFile(outputdirectory);
	}
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        
        if(args==null)System.exit(0);
        if( args.length <2)
        {
            System.out.println( "less argument...>" );
            System.exit( 0 );
        }

        computeRelativeFrequency( args[ 0 ],args[1] );
    }
}///home/ridwan/workspace/RelFreq/src/main/resources/inpu
