package qlobbe;

/*
 * Java
 */

import java.util.Map;
import java.util.HashMap;

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
 * Spark-Solr
 */

import com.lucidworks.spark.util.SolrSupport;
import com.lucidworks.spark.SparkApp;

/*
 * Solr
 */

import org.apache.solr.common.SolrInputDocument;

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

		JavaPairRDD<String, Map<String, String>> metaDataGrouped = metaData.mapToPair(
		  	new PairFunction<Tuple2<BytesWritable, StreamableDAFFRecordWritable>, String, StreamableDAFFRecordWritable>() {
		    	public Tuple2<String, StreamableDAFFRecordWritable> call(Tuple2<BytesWritable, StreamableDAFFRecordWritable> c) {
		     	 	return new Tuple2<String, StreamableDAFFRecordWritable>(((RecordHeader)c._2.get()).id(), c._2);
		   		}
			})
			.filter(c -> {
				Record r =	(Record)((RecordHeader)c._2.get());
				return r.content() instanceof MetadataContent ? true : false;
			}).mapValues(v -> {
				Record r =	(Record)((RecordHeader)v.get());
				return ((MetadataContent)r.content()).getMetadata();
			}).combineByKey(v -> {
				Map<String, String> x = new HashMap<String, String>();
        		x.put("corpus", v.get("corpus"));
        		x.put("url", v.get("url"));
        		x.put("date", v.get("date"));
        		return x;
			},(x,v) -> {
				x.put("date", x.get("date") + "|" + v.get("date"));
				return x;
			},(x,y) -> {
				x.put("date", x.get("date") + "|" + y.get("date"));
				return x;
			});

		JavaRDD<SolrInputDocument> docs = metaDataGrouped.map( c -> {
			SolrInputDocument doc = new SolrInputDocument();
			doc.remove("_indexed_at_tdt");
			doc.addField("ID",c._1);
			doc.addField("URL",c._2.get("url"));
			doc.addField("SITE","toto.com");
			return doc;
		});

		SolrSupport.indexDocs("localhost:2181", "ediasporas_maroco", 10, docs);

		System.out.println("wesh");

		// metaDataGrouped.foreach(c -> {System.out.println(c._2.toString());});		

	    sc.close();

	    System.out.println("Done !");
  	}
}