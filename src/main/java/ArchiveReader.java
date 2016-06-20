package qlobbe;

/*
 * Java
 */

import java.util.Date;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

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
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.HashPartitioner;

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

	public static void archiveToSolr(String metaPath, String dataPath, ArrayList<String> corpus, DateFormat df) {

		/*
		 * Run a local spark job with 2 threads
		 */ 

	    // SparkConf conf = new SparkConf().setMaster("local[20]").setAppName("ArchiveReader");

	    SparkConf conf = new SparkConf().setAppName("ArchiveReader");
	    
	    JavaSparkContext sc = new JavaSparkContext(conf);

	    /*
	     * Process DAFF file as a JavaPairRDD entity
	     */

	    Configuration jobConf = new Configuration();

        JavaPairRDD<BytesWritable, StreamableDAFFRecordWritable> metaData =  sc.newAPIHadoopFile(metaPath,StreamableDAFFInputFormat.class,BytesWritable.class,StreamableDAFFRecordWritable.class, jobConf);

		System.out.println("=====> Process MetaData");

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
			}).partitionBy(new HashPartitioner(30)).reduceByKey((u,v) -> {
				Map<String, String> x = new HashMap<String, String>();
       			x.put("active",v.get("active"));
       			x.put("client_country", v.get("client_country"));
        		x.put("client_ip", v.get("client_ip"));
        		x.put("client_lang", v.get("client_lang"));
        		x.put("corpus", v.get("corpus"));
        		// agregate date
        		x.put("crawl_session", u.get("crawl_session") + "____" + v.get("crawl_session"));
        		x.put("date", u.get("date") + "____" + v.get("date"));
        		x.put("ip", v.get("ip"));
        		x.put("length", v.get("length"));
        		x.put("level", v.get("level"));
        		x.put("page", v.get("page"));
        		x.put("referer_url", v.get("referer_url"));
        		x.put("site",getSite(v.get("crawl_session")));
        		x.put("type",v.get("type"));
        		x.put("url", v.get("url"));        		
        		return x;
			},3);

        metaDataRDD.count();

		System.out.println("=====> Process Data");

        JavaPairRDD<BytesWritable, StreamableDAFFRecordWritable> data =  sc.newAPIHadoopFile(dataPath,StreamableDAFFInputFormat.class,BytesWritable.class,StreamableDAFFRecordWritable.class, jobConf);

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
					x.put("content",new String(content));
		     	 	return new Tuple2<String, Map<String, String>>(id, x);
		   		}
			}
		);

		dataRDD.partitionBy(new HashPartitioner(30));

		dataRDD.count();

		System.out.println("=====> Join Data & Meta ...");

		JavaRDD<SolrInputDocument> docs = metaDataRDD.join(dataRDD).mapToPair(c -> {
			
			Map<String, String> m = (Map<String, String>)c._2._1;
			
			String content = c._2._2.get("content"); 

			String site = m.get("site");
			
			Pattern pattern = Pattern.compile("href=\"http://[^\"]*");
    		
    		Matcher matcher = pattern.matcher(content);

    		// url from the current web site to himself
    		ArrayList<String> link_self = new ArrayList<String>();
    		// url from the current web site to a web site of his diasporas
    		ArrayList<String> link_dias = new ArrayList<String>();
    		// url from the web site to a web out of his diasporas
    		ArrayList<String> link_unkn = new ArrayList<String>();

    		ArrayList<String> link_dias_code = new ArrayList<String>();

    		corpus.forEach(s -> {
    			link_dias_code.add("0");
    		});

		    while (matcher.find()) {
		      // Get the matching string
		      String url = matcher.group().substring(13);
		      String name = (String)(url.split("/")[0]).replace("www.","");

		      if (url.contains(".css"))
		      	break;

		      if (name.equals(site)) {
		      	link_self.add(url);
		      } else if (corpus.contains(name)) {
		      	link_dias.add(url);
		      	int idx = corpus.indexOf(name);
		      	link_dias_code.set(idx,"1");
		      } else {
		      	link_unkn.add(url);
		      }
		    }	

		    String link = link_dias_code.stream().reduce((x,y) -> x + y).get();

		    // m.put("link_self",link_self.toString());
		    // m.put("link_dias",link_dias.toString());
		    // m.put("link_unkn",link_unkn.toString());
		    // m.put("content",content);		

		    m.put("link",link);
			
			return new Tuple2<String, Map<String, String>>(c._1, m);
		}).map( c -> {
			SolrInputDocument doc = new SolrInputDocument();
			doc.addField("id",c._1);
			doc.addField("active",((String)c._2.get("active")).equals("1") ? true : false);
   			doc.addField("client_country", c._2.get("client_country"));
    		doc.addField("client_ip", c._2.get("client_ip"));
    		doc.addField("client_lang", (c._2.get("client_lang")).split(", "));
    		doc.addField("corpus", c._2.get("corpus"));
    		
    		/*
    		 * dates processing
    		 */
    		
    		String[] dates = c._2.get("date").split("____");

    		String[] crawl_session = c._2.get("crawl_session").split("____");

    		String[] crawl_session_dates = Arrays.stream(crawl_session).map( cs -> { 
    			cs = cs.split("@")[1];
    			String d = cs.substring(0,4) + '-' + cs.substring(4,6) + '-' + cs.substring(6,11) + ':' + cs.substring(11,13) + ':' + cs.substring(13);
    			return d; 
    		}).toArray(size -> new String[size]);

    		doc.addField("date",dates);    		
    		doc.addField("first_modified",dates[0]);
    		doc.addField("last_modified",dates[dates.length - 1]);
    		
    		doc.addField("crawl_session",crawl_session);
    		doc.addField("first_crawl_session",crawl_session[0]);
    		doc.addField("last_crawl_session",crawl_session[crawl_session.length - 1]);

    		doc.addField("crawl_session_date",crawl_session_dates);
    		doc.addField("first_crawl_session_date",crawl_session_dates[0]);
			doc.addField("last_crawl_session_date",crawl_session_dates[crawl_session_dates.length - 1]);

			doc.addField("link_diaspora",c._2.get("link"));

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

		System.out.println("=====> Start Indexing ...");		

		SolrSupport.indexDocs("lame11:2181", "ediasporas_maroco", 1000, docs);

	    sc.close();

	    System.out.println("Done !");
	}

	public static void main(String[] args) {

		ObjectMapper mapper = new ObjectMapper();

		String metaPath = "/user/qlobbe/ediasporas_Marocains/metadata-r-00006.daff";

		String dataPath = "/user/qlobbe/ediasporas_Marocains/data-r-00006.daff";		

		String sitePath = "/infres/ir500/ic2+/qlobbe/data/ediasporas-corpus/ediasporas_Marocains/site.txt";

    	ArrayList<String> corpus = new ArrayList<String>();		

    	/*
    	 * Parse site.txt file
    	 */

		try (Stream<String> stream = Files.lines(Paths.get(sitePath))) {
		
			stream.forEach(s -> {
				corpus.add(s);
			});
		
		} catch (IOException e) {
		
			e.printStackTrace();
		
		}		

		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

		archiveToSolr(metaPath, dataPath, corpus, df);
		
  	}
}