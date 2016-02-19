package qlobbe;

/*
 * Spark
 */

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

public class ArchiveReader {

	public static void main(String[] args) {
    
		/*
		 * Run a local spark job with 2 threads
		 */ 

	    SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("ArchiveReader");
	    JavaSparkContext sc = new JavaSparkContext(conf);

	    sc.close();

	    System.out.println("Coucou");
  	}
}