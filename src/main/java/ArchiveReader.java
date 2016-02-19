package qlobbe;

/*
 * Java
 */

import java.util.Map;

/*
 * Scala
 */

import scala.Tuple2;

/*
 * Spark
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

/*
 * Hadoop
 */

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.conf.Configuration;

/*
 * Jackson
 */

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/*
 * Ina
 */

import fr.ina.dlweb.daff.MetadataContent;
import fr.ina.dlweb.daff.DAFFUtils;
import fr.ina.dlweb.daff.Record;
import fr.ina.dlweb.daff.RecordHeader;
import fr.ina.dlweb.hadoop.io.StreamableDAFFInputFormat;
import fr.ina.dlweb.hadoop.io.StreamableDAFFRecordWritable;


public class ArchiveReader {

	public static void main(String[] args) {

		ObjectMapper mapper = new ObjectMapper();

		String archivePath = "/home/qlobbe/data/ediaspora-corpus/ediaspora_Marocains/metadata-small.daff";

		/*
		 * Run a local spark job with 2 threads
		 */ 

	    SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("ArchiveReader");
	    
	    JavaSparkContext sc = new JavaSparkContext(conf);

	    /*
	     * Process DAFF file as a JavaPairRDD entity
	     */

	    Configuration jobConf = new Configuration();

        JavaPairRDD<BytesWritable, StreamableDAFFRecordWritable> metaData =  sc.newAPIHadoopFile(archivePath,StreamableDAFFInputFormat.class,BytesWritable.class,StreamableDAFFRecordWritable.class, jobConf);

   //      JavaPairRDD<byte[], StreamableDAFFRecordWritable> metaDataMap = metaData.mapToPair(
		 //  	new PairFunction<Tuple2<BytesWritable, StreamableDAFFRecordWritable>, byte[], StreamableDAFFRecordWritable>() {
		 //    	public Tuple2<byte[], StreamableDAFFRecordWritable> call(Tuple2<BytesWritable, StreamableDAFFRecordWritable> c) {
		 //     	 	return new Tuple2<byte[], StreamableDAFFRecordWritable>(c._1.copyBytes(), c._2);
		 //   		}
			// });

		JavaPairRDD<byte[], Map<String, String>> metaDataMap = metaData.mapToPair(
		  	new PairFunction<Tuple2<BytesWritable, StreamableDAFFRecordWritable>, byte[], StreamableDAFFRecordWritable>() {
		    	public Tuple2<byte[], StreamableDAFFRecordWritable> call(Tuple2<BytesWritable, StreamableDAFFRecordWritable> c) {
		     	 	return new Tuple2<byte[], StreamableDAFFRecordWritable>(c._1.copyBytes(), c._2);
		   		}
			})
			.filter(c -> {

				Record r =	(Record)((RecordHeader)c._2.get());

				if ( r.content() instanceof MetadataContent ) {
        			return true;
        		} else {
        			return false;
        		}

			}).mapValues(v -> {

					Record r =	(Record)((RecordHeader)v.get());
					return ((MetadataContent)r.content()).getMetadata();

			});        

			metaDataMap.foreach(c -> {System.out.println(c._2.toString());});

   //      metaDataMap.foreach(c -> {



   //      		Record r =	(Record)((RecordHeader)c._2.get());

   //      		if ( r.content() instanceof MetadataContent ) {
   //      			Map<String, String> map = ((MetadataContent)r.content()).getMetadata();

   //      			System.out.println(map.toString());
   //      		}

   //      		// String dataString = DAFFUtils.dataToString(((MetadataContent)r.content()).getMetadata(), "UTF-8");
			// });

	    sc.close();

	    System.out.println("Coucou");
  	}
}