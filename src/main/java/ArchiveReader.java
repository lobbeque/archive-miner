package qlobbe;

/*
 * Java
 */

import java.util.Date;
import java.util.Arrays;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

import fr.ina.dlweb.daff.Content;
import fr.ina.dlweb.daff.DataContent;
import fr.ina.dlweb.daff.MetadataContent;
import fr.ina.dlweb.daff.DAFFUtils;
import fr.ina.dlweb.daff.Record;
import fr.ina.dlweb.daff.RecordHeader;
import fr.ina.dlweb.hadoop.io.StreamableDAFFInputFormat;
import fr.ina.dlweb.hadoop.io.StreamableDAFFRecordWritable;


public class ArchiveReader {

	/*
	 * Get the web site from the crawl session
	 */
	public static String getSite(String crawl_session) {

		return ((String[])crawl_session.split("@"))[0];

	}

	public static void main(String[] args) {

		ObjectMapper mapper = new ObjectMapper();

		String archivePath = "/home/qlobbe/data/ediaspora-corpus/ediaspora_Marocains/metadata-small.daff";

		String dataPath    = "/home/qlobbe/data/ediaspora-corpus/ediaspora_Marocains/data-small.daff";

		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

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

        JavaPairRDD<BytesWritable, StreamableDAFFRecordWritable> data =  sc.newAPIHadoopFile(dataPath,StreamableDAFFInputFormat.class,BytesWritable.class,StreamableDAFFRecordWritable.class, jobConf);

		System.out.println("=====> Process Data ...");


		data.filter(c -> {
			Record r =	(Record)((RecordHeader)c._2.get());			
			return r.content() instanceof DataContent ? true : false;
		}).foreach(c -> {
			String id = ((RecordHeader)c._2.get()).id();
			System.out.println("==== " + id);
			Record r =	(Record)((RecordHeader)c._2.get());
			byte[] d = ((DataContent)r.content()).get();
			String s = new String(d);
			Pattern pattern = Pattern.compile("href=\"http://[^\"]*");
    		Matcher matcher = pattern.matcher(s);
    		// Find all matches
		    while (matcher.find()) {
		      // Get the matching string
		      String match = matcher.group();
		      System.out.println(match);
		    }
		});

		/*
		 * Write Data RDD like a JavaPairRDD<String, Map<String, String>> = < sha key, html content > 
		 */

		JavaPairRDD<String, Map<String, String>> dataRDD =	data.filter(c -> {
			Record r =	(Record)((RecordHeader)c._2.get());			
			return r.content() instanceof DataContent ? true : false; 
		}).mapToPair(
		  	new PairFunction<Tuple2<BytesWritable, StreamableDAFFRecordWritable>, String, Map<String, String>>() {
		    	public Tuple2<String, Map<String, String>> call(Tuple2<BytesWritable, StreamableDAFFRecordWritable> c) throws IOException {
		    		String id = ((RecordHeader)c._2.get()).id();
					Record r =	(Record)((RecordHeader)c._2.get());
					byte[] content = ((DataContent)r.content()).get();
					Map<String, String> x = new HashMap<String, String>();
		     	 	return new Tuple2<String, Map<String, String>>(id, x.put("content",new String(content)));
		   		}
			}
		)	

		System.out.println("=====> Process Meta ...");

		/*
		 * Write MetaData RDD like a JavaPairRDD<String, Map<String, String>> = < sha key, meta properties > agregated by sha key 
		 */

		JavaPairRDD<String, Map<String, String>> metaDataRDD = metaData.filter(c -> {
				Record r =	(Record)((RecordHeader)c._2.get());
				return r.content() instanceof MetadataContent ? true : false;
			}).mapToPair(
		  	new PairFunction<Tuple2<BytesWritable, StreamableDAFFRecordWritable>, String, Map<String, String>>() {
		    	public Tuple2<String, Map<String, String>> call(Tuple2<BytesWritable, StreamableDAFFRecordWritable> c) throws IOException {
		    		Record r = (Record)((RecordHeader)c._2.get());
		    		Map<String, String> m = ((MetadataContent)r.content()).getMetadata();
		     	 	return new Tuple2<String, Map<String, String>>(m.get("content"), m);
		   		}
			}).combineByKey(v -> {
				Map<String, String> x = new HashMap<String, String>();
       			x.put("active",v.get("active"));
       			x.put("client_country", v.get("client_country"));
        		x.put("client_ip", v.get("client_ip"));
        		x.put("client_lang", v.get("client_lang"));
        		x.put("corpus", v.get("corpus"));
        		x.put("crawl_session",v.get("crawl_session"));
        		x.put("date", v.get("date"));
        		x.put("ip", v.get("ip"));
        		x.put("length", v.get("length"));
        		x.put("level", v.get("level"));
        		x.put("page", v.get("page"));
        		x.put("referer_url", v.get("referer_url"));
        		x.put("site",getSite(v.get("crawl_session")));
        		x.put("type",v.get("type"));
        		x.put("url", v.get("url"));        		
        		return x;
			},(x,v) -> {
				x.put("crawl_session", x.get("crawl_session") + "____" + v.get("crawl_session"));
				x.put("date", x.get("date") + "____" + v.get("date"));
				return x;
			},(x,y) -> {
				x.put("crawl_session", x.get("crawl_session") + "____" + y.get("crawl_session"));
				x.put("date", x.get("date") + "____" + y.get("date"));
				return x;
			});
		
		/*
		 * Join Data & Meta
		 */

		metaDataRDD.union(dataRDD).reduceByKey((x,y) -> {
			x.put("content",y.get("content"));
			return x;
		});

		System.out.println("=====> SolrInputDocument");

		JavaRDD<SolrInputDocument> docs = metaDataRDD.map( c -> {
			SolrInputDocument doc = new SolrInputDocument();
			doc.addField("id",c._1);
			doc.addField("active",((String)c._2.get("active")).equals("1") ? true : false);
   			doc.addField("client_country", c._2.get("client_country"));
    		doc.addField("client_ip", c._2.get("client_ip"));
    		doc.addField("client_lang", (c._2.get("client_lang")).split(", "));
    		doc.addField("corpus", c._2.get("corpus"));
    		doc.addField("crawl_session",c._2.get("crawl_session").split("____"));
    		doc.addField("date",c._2.get("date").split("____"));    		
    		doc.addField("ip", c._2.get("ip"));
    		doc.addField("length", Double.parseDouble(c._2.get("length")));
    		doc.addField("level", Integer.parseInt(c._2.get("level")));
    		doc.addField("page", Integer.parseInt(c._2.get("page")));
    		doc.addField("referer_url", c._2.get("referer_url"));
    		doc.addField("site",c._2.get("site"));
    		doc.addField("type",c._2.get("type"));
    		doc.addField("url", c._2.get("url"));        		
			return doc;
		});

		System.out.println("=====> Indexation");

		// SolrSupport.indexDocs("localhost:2181", "ediasporas_maroco", 10, docs);

		// metaDataRDD.foreach(c -> {
		// 	System.out.println(c._2.get("date"));
		// });		

	    sc.close();

	    System.out.println("Done !");
  	}
}