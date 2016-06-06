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

	public static void archiveToSolr(String archivePath, String dataPath, ArrayList<String> corpus, DateFormat df) {

		/*
		 * Run a local spark job with 2 threads
		 */ 

	    SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("ArchiveReader").set("spark.driver.memory", "10g").set("spark.executor.memory","10g");
	    
	    JavaSparkContext sc = new JavaSparkContext(conf);

	    /*
	     * Process DAFF file as a JavaPairRDD entity
	     */

	    Configuration jobConf = new Configuration();

        JavaPairRDD<BytesWritable, StreamableDAFFRecordWritable> metaData =  sc.newAPIHadoopFile(archivePath,StreamableDAFFInputFormat.class,BytesWritable.class,StreamableDAFFRecordWritable.class, jobConf);

        JavaPairRDD<BytesWritable, StreamableDAFFRecordWritable> data =  sc.newAPIHadoopFile(dataPath,StreamableDAFFInputFormat.class,BytesWritable.class,StreamableDAFFRecordWritable.class, jobConf);

		System.out.println("=====> Process Data ...");

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
					x.put("content",new String(content));
		     	 	return new Tuple2<String, Map<String, String>>(id, x);
		   		}
			}
		);	

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
		
		System.out.println("=====> Join Data & Meta ...");

		/*
		 * Join Data & Meta using sha key
		 */

		// joinDataRDD.foreach(c -> {
		// 	// System.out.println("====================");
		// 	// System.out.println(c._2.get("link"));
		// 	// System.out.println(c._2.get("link_dias"));
		// 	// System.out.println(c._2.get("link_unkn"));
		// });

		System.out.println("=====> SolrInputDocument");

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

		System.out.println("=====> Indexation");

		// SolrSupport.indexDocs("localhost:2181", "ediasporas_maroco", 10, docs);

		SolrSupport.indexDocs("localhost:2181", "ediasporas_maroco", 10, docs);

		// metaDataRDD.foreach(c -> {
		// 	System.out.println(c._2.get("date"));
		// });		

	    sc.close();

	    System.out.println("Done !");
	}

	public static void archiveStreamToSolr(String archivePath, String dataPath, ArrayList<String> corpus, DateFormat df) {
		
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("ArchiveReader").set("spark.driver.memory", "10g").set("spark.executor.memory","10g");
		
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

		Configuration jobConf = new Configuration();

		JavaPairInputDStream<BytesWritable, StreamableDAFFRecordWritable> metaData = jssc.fileStream(archivePath,BytesWritable.class,StreamableDAFFRecordWritable.class,StreamableDAFFInputFormat.class);	

		JavaPairInputDStream<BytesWritable, StreamableDAFFRecordWritable> data = jssc.fileStream(dataPath,BytesWritable.class,StreamableDAFFRecordWritable.class,StreamableDAFFInputFormat.class);	

		JavaPairDStream<String, Map<String, String>> dataRDD =	data.filter(c -> {
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

		dataRDD.print();

		JavaPairDStream<String, Map<String, String>> metaDataRDD = metaData.filter(c -> {
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
			}, new HashPartitioner(1) );
		
		System.out.println("=====> Join Data & Meta ...");

		JavaDStream<SolrInputDocument> docs = metaDataRDD.join(dataRDD).mapToPair(c -> {
			
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

    		System.out.println("coucou");

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

    		System.out.println("youyou");

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

		docs.print();

		SolrSupport.indexDStreamOfDocs("localhost:2181", "ediasporas_maroco", 100, docs);	
		
		jssc.start();	

		jssc.awaitTermination();

		System.out.println("Done !");
	}

	public static void main(String[] args) {

		ObjectMapper mapper = new ObjectMapper();

		// String archivePath = "/home/qlobbe/data/ediaspora-corpus/ediaspora_Marocains/meta";
		// String dataPath    = "/home/qlobbe/data/ediaspora-corpus/ediaspora_Marocains/data";
		String archivePath = "/home/qlobbe/data/ediaspora-corpus/ediaspora_Marocains/metadata-small.daff";
		String dataPath    = "/home/qlobbe/data/ediaspora-corpus/ediaspora_Marocains/data-small.daff";		
		String sitePath    = "/home/qlobbe/data/ediaspora-corpus/ediaspora_Marocains/site.txt";

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

		archiveToSolr(archivePath, dataPath, corpus, df);

		// archiveStreamToSolr(archivePath, dataPath, corpus, df);

		
  	}
}