package java_spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;



public class Javaspark_test {
	
	public static void main(String[] args) {
		
      
        SparkConf conf = new SparkConf().setAppName("max_dislike").setMaster("local");
   
        JavaSparkContext sc = new JavaSparkContext(conf);
       
        JavaRDD<String> lines = sc.textFile(args[0]);
        
    
        JavaPairRDD<String, String> k_v_pairs = lines.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String line) throws Exception {
            	
                final String[] fields = line.split(",");
                final String key_1 = fields[0]+"^"+fields[11];
                final String key_2 = fields[1]+"^"+fields[3]+"^"+fields[6]+"^"+fields[7];
                return new Tuple2<>(key_1, key_2);
            }
        });
        
      
        

        
        JavaPairRDD<String, Iterable<String>> group_result  = k_v_pairs.groupByKey();
        
        JavaPairRDD<Integer, String> result = group_result.mapToPair(new PairFunction<Tuple2<String,Iterable<String>>, Integer, String>() {
        	
            @Override
            public Tuple2<Integer, String> call(Tuple2<String,Iterable<String>> pairs) throws Exception{
            	
            	Iterable<String> iter = pairs._2();
            	HashMap<Integer, String> vaue_pair = new HashMap<>();
            	ArrayList<Integer> trending_date = new ArrayList<Integer>();
            	for(String i:iter) {
            		String time = i.split("\\^")[0];
            		
            		if(time.split("\\.").length==3) {
            		int trending_date_value = Integer.parseInt(time.split("\\.")[0]+time.split("\\.")[2]+time.split("\\.")[1]);
            		trending_date.add(trending_date_value);            		
            		vaue_pair.put(trending_date_value, i);
            		}
            		Collections.sort(trending_date);
            	}
            	
            	if (trending_date.size()>1){
            		int frist_time = trending_date.get(0);
            		int second_time = trending_date.get(1);
            		int first_trending_gap =  Integer.parseInt(vaue_pair.get(frist_time).split("\\^")[3]) -Integer.parseInt(vaue_pair.get(frist_time).split("\\^")[2]);
            		int second_trending_gap =  Integer.parseInt(vaue_pair.get(second_time).split("\\^")[3]) -Integer.parseInt(vaue_pair.get(second_time).split("\\^")[2]);
            		int growth = second_trending_gap-first_trending_gap;
           
            		String tmp_result = vaue_pair.get(frist_time).split("\\^")[1]+"|"+pairs._1().split("\\^")[0]+"|"+pairs._1().split("\\^")[1];
            		return new Tuple2(growth,tmp_result);
            	}
            	
            	return new Tuple2(0,"0");
          	     	
        }
     	
		});
        
        JavaPairRDD<Integer, String> sorted = result.sortByKey(false);
        
     
      
        sorted.saveAsTextFile(args[1]);
    
       sc.stop();
	}
	
	
}
