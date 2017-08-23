package qlobbe;

/*
 * Java
 */

import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;

import java.net.HttpURLConnection;
import java.net.URL;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import java.security.MessageDigest;

import java.text.SimpleDateFormat;
import java.text.DateFormat;
import java.text.ParseException;

import java.util.Date;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.util.Locale;

/*
 * Scala
 */

import scala.Tuple2;
import scala.Option;
import scala.Predef;

/*
 * Spark
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.HashPartitioner;
import org.apache.spark.broadcast.*;
import org.apache.spark.rdd.RDD;


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
 * Json
 */

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

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

/*
 * Guava 
 */

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.common.hash.Funnel;

/*
 * Rivelaine
 */

import qlobbe.Rivelaine;

public class ArchiveReader {

	private String pattern_date = "(?:(?:31(\/|-|\.)(?:0?[13578]|1[02]|(?:Jan|Mar|May|Mai|Jul|Juillet|Aug|Aou|Oct|Dec)))\1|(?:(?:29|30)(\/|-|\.)(?:0?[1,3-9]|1[0-2]|(?:Jan|Mar|Apr|Avr|May|Mai|Jun|Juin|Jul|Aug|Aou|Sep|Oct|Nov|Dec))\2))(?:(?:1[6-9]|[2-9]\d)?\d{2})$|^(?:29(\/|-|\.)(?:0?2|(?:Feb))\3(?:(?:(?:1[6-9]|[2-9]\d)?(?:0[48]|[2468][048]|[13579][26])|(?:(?:16|[2468][048]|[3579][26])00))))$|^(?:0?[1-9]|1\d|2[0-8])(\/|-|\.)(?:(?:0?[1-9]|(?:Jan|Feb|Fev|Mar|Apr|Avr|May|Mai|Jun|Juin|Jul|Juillet|Aou|Aug|Sep))|(?:1[0-2]|(?:Oct|Nov|Dec)))\4(?:(?:1[6-9]|[2-9]\d)?\d{2})";

	/*
	 * Get the web site from the crawl session
	 */
	public static String getSite(String crawl_session) {

		return ((String[])crawl_session.split("@"))[0];

	}

	// Get a List from a json array
	public static List<String> jsonToStringList (JSONArray arr) {
		List<String> list = new ArrayList<String>();
		if (arr != null) {
			for (int i = 0; i < arr.length(); i++) {
			    list.add(arr.getString(i));
			}			
		}
		return list;
	}	

	// Get an array list from a path to file
	public static ArrayList<String> fileToStringArrList (String path) {
    	ArrayList<String> arr = new ArrayList<String>();		
		try (Stream<String> stream = Files.lines(Paths.get(path))) {
			stream.forEach(s -> {
				arr.add(s);
			});
		} catch (IOException e) {
			e.printStackTrace();
		}	
		return arr;	
	}

	/*
	 * encode a String with an sha-256 hash
	 */

	public static String getShaKey(String msg) throws Exception {

		try {

			MessageDigest md = MessageDigest.getInstance("SHA-256");

			md.update(msg.getBytes("UTF-8"));

			byte[] byteData = md.digest();

			//convert the byte to hex format
	        
	        StringBuffer sb = new StringBuffer();

	        for (int i = 0; i < byteData.length; i++) {
	         sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
	        }

	        return sb.toString();		
		
		} catch(Exception e) {
        
        	return null;
    	
    	}
    		
	}

	// public static void indexEvent(
	// 				JavaPairRDD<String, Map<String, Object>> metaDataRDD,
	// 				JavaPairRDD<String, Map<String, Object>> dataRDD,
	// 				int partitionSize
	// 			) {

	// 	metaDataRDD.join(dataRDD)
	// 	.map(c -> {
	// 		HashMap<String, Object> tmp = new HashMap<String, Object>(); 
	// 		String title = (String) c._2._2.get("page_title");
	// 		String url   = (String) c._2._1.get("page_url");
	// 		List<Date> fragDate = ((List<Map<String,Object>>)c._2._2.get("page_content")).stream()
	// 			.filter(frag -> (Date)frag.get("date") != null)
	// 			.map(frag -> (Date)frag.get("date")).collect(Collectors.toList());

	// 		final Date minFragDate;
	// 		if(fragDate.isEmpty()) {
	// 		   minFragDate = null;
	// 		} else {
	// 		   minFragDate = fragDate.stream().min((d1,d2) -> ((Date)d1).compareTo((Date)d2)).get();
	// 		}
	// 		String id = "";
	// 		try {
 //    			id = getShaKey(title);
 //    		} catch(Exception e) {
 //    			id = "";
 //    		}

 //    		tmp.put("id", id);
 //    		tmp.put("title", title);
 //    		tmp.put("url", url);
 //    		tmp.put("date", minFragDate);

 //    		return tmp;
	// 	}).filter(c -> {
	// 		return c.get("id") != "" && c.get("title") != null && c.get("date") != null;
	// 	}).map(c -> {
	// 		TimeZone tz = TimeZone.getTimeZone("UTC");
	// 		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'"); // Quoted "Z" to indicate UTC, no timezone offset
	// 		df.setTimeZone(tz);
	// 		return (String)c.get("id") + ";" + ((String)c.get("title")).replaceAll(";",",") + ";" + (String)c.get("url") + ";" + df.format((Date)c.get("date"));
	// 	}).saveAsTextFile("file:///cal/homes/qlobbe/tmp/news");

	//     sc.close();

	// }	

	// Process an http get req (> https://www.mkyong.com/java/how-to-send-http-request-getpost-in-java/ )
	public static JSONArray httpGet(String url, String target) {
		HttpURLConnection req = (HttpURLConnection) (new URL(url + "source=" + target)).openConnection();

		req.setRequestMethod("GET");
		req.setRequestProperty("User-Agent", "Mozilla/5.0");

		String tmp;
		BufferedReader in = new BufferedReader(new InputStreamReader(req.getInputStream()));
		StringBuffer buff = new StringBuffer();
		while ((tmp = in.readLine()) != null) {
			buff.append(tmp);
		}
		in.close();

		JSONArray resp = (JSONArray)parser.parse(buff.toString());
		return resp;		
	} 

	public static JavaPairRDD<String, Map<String, Object>> getMetaDataRDD(
					JavaPairRDD<BytesWritable, StreamableDAFFRecordWritable> data,
					int partitionSize,
					List<String> urlFilter
						) {

		JavaPairRDD<String, Map<String, Object>> metaDataRDD = metaData.filter(c -> {
				// Drop DAFF header
				Record r =	(Record)((RecordHeader)c._2.get());
				return r.content() instanceof MetadataContent ? true : false;
			}).filter(c -> {
				// Keep meta from a given site
		    	Record r = (Record)((RecordHeader)c._2.get());
		    	Map<String, Object> m = (Map)((MetadataContent)r.content()).getMetadata();
		    	String url = (String)m.get("url");	
		    	String domain = ((String[])((String)m.get("crawl_session")).split("@"))[0];
		    	return (urlFilter.isEmpty() ? true : urlFilter.contains(domain)) && ! url.contains("/forum/archive/") ? true : false;
			}).mapToPair(
				// Change type of PairRDD
		  		new PairFunction<Tuple2<BytesWritable, StreamableDAFFRecordWritable>, String, Map<String, Object>>() {
		    		public Tuple2<String, Map<String, Object>> call(Tuple2<BytesWritable, StreamableDAFFRecordWritable> c) throws IOException {
		    			Record r = (Record)((RecordHeader)c._2.get());
		    			Map<String, Object> m = (Map)((MetadataContent)r.content()).getMetadata();
		     	 		return new Tuple2<String, Map<String, Object>>((String)m.get("content"), m);
		   			}
				}
			).filter(c -> {	
				// Drop out of date range 
 				SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    			Date dlDate = df.parse(((String)c._2.get("date")).split("T")[0]);
				return (dlDate.after(df.parse(dateFrom)) || dlDate.equals(df.parse(dateFrom))) && dlDate.before(df.parse(dateTo));
			}).reduceByKey((u,v) -> {
				// Group meta by key (ie: by archived version)				
				Map<String, Object> x = new HashMap<String, Object>();
       			x.put("archive_active",    v.get("active"));
       			x.put("archive_corpus",    v.get("corpus"));
       			x.put("archive_mime_type", v.get("type"));
       			x.put("archive_country",   v.get("client_country"));
        		x.put("archive_lang",      v.get("client_lang"));
        		// agregate date
        		x.put("crawl_id",          u.get("crawl_session") + "|" + v.get("crawl_session"));
        		x.put("page_date",         u.get("date") + "|" + v.get("date"));
        		x.put("page_url",          v.get("url"));        		
        		return x;
			}).partitionBy(new HashPartitioner(partitionSize));

		return metaDataRDD;

	}

	public static JavaPairRDD<String, Map<String, Object>> getDataRDD(
					JavaPairRDD<BytesWritable, StreamableDAFFRecordWritable> metaData,
					Broadcast<BloomFilter<CharSequence>> siteIdsBroadcast,
					int partitionSize
						) {

		JavaPairRDD<String, Map<String, Object>> dataRDD =	data.filter(c -> {
			// Keep data linked to metadata
			Record r =	(Record)((RecordHeader)c._2.get());	
			String id = r.id();		
			return r.content() instanceof DataContent && siteIdsBroadcast.value().mightContain(id); 
		})	
		.mapToPair(
			// Change type of PairRDD
		  	new PairFunction<Tuple2<BytesWritable, StreamableDAFFRecordWritable>, String, byte[]>() {
		    	public Tuple2<String, byte[]> call(Tuple2<BytesWritable, StreamableDAFFRecordWritable> c) throws IOException {
		     	 	return new Tuple2<String, byte[]>((String)((RecordHeader)c._2.get()).id(), ((DataContent)((Record)((RecordHeader)c._2.get())).content()).get());
		   		}
		   	}
		)
		.repartition(partitionSize)
		.mapValues(v -> {
			
			// Extract info & fragment from pages
			
			Map<String, Object> x = new HashMap<String, Object>();

			// Use Rivelaine.scala to extract meta info
		    
		    Map<String, Object> header = Rivelaine.getHeaderJava(new String(v),"file");
			
			x.put("page_title",       (String)header.get("title"));			    	 
			x.put("page_description", (String)content.get("description"));
		    x.put("page_publisher",   (String)content.get("publisher"));

		    String published_date = (String)content.get("published_date");

		    if (published_date != "" && Pattern.matches(pattern_date, published_date)) {
		    	x.put("page_published_date", Rivelaine.normalizeDate(published_date);
		    }

		    // Use Rivelaine.js server to extract fragments 

		    List<HashMap<String,Object>> fragments = ((JSONArray)httpGet(rivelaineUrl,new String(v))).stream().map(frag -> {

				HashMap<String, Object> tmp = new HashMap<String, Object>();

				tmp.put("type",   (List<String>)(jsonToList((JSONArray)frag.get("type")));
				tmp.put("author", (List<String>)(jsonToList((JSONArray)frag.get("author")));
				tmp.put("href",   (List<String>)(jsonToList((JSONArray)frag.get("href")));
				tmp.put("node",   (List<String>)(jsonToList((JSONArray)frag.get("node")));
				tmp.put("nodeId", (List<String>)(jsonToList((JSONArray)frag.get("nodeId")));
				tmp.put("ratio",       (Integer)(frag.get("ratio"));
				tmp.put("offset",      (Integer)(frag.get("offset"));
				tmp.put("text",        (Integer)(frag.get("text"));		    	
				tmp.put("date",   (List<String>)(frag.get("date").stream().map(d -> {
					if (d != "" && Pattern.matches(pattern_date,d)) {
						return Rivelaine.normalizeDate((String)d)
					}
				}));

				return tmp;

		    });

			x.put("fragments",fragments);

		});

		return dataRDD;
	}

	public static SolrInputDocument cleanDoc(SolrInputDocument doc){
		doc.removeField("id");
		doc.removeField("frag_type");
		doc.removeField("frag_author");
		doc.removeField("frag_date");
		doc.removeField("frag_date_first");
		doc.removeField("frag_date_level");
		doc.removeField("frag_href");
		doc.removeField("frag_href_id");
		doc.removeField("frag_ratio");
		doc.removeField("frag_node");
		doc.removeField("frag_offset");
		doc.removeField("frag_text");
		doc.removeField("frag_text_id");
		return doc;		
	}

	public static JavaRDD<SolrInputDocument> getSolrDocs(
						JavaPairRDD<String, Map<String, Object>> metaDataRDD,
						JavaPairRDD<String, Map<String, Object>> dataRDD,
						int partitionSize
							){

		JavaRDD<SolrInputDocument> docs = metaDataRDD.join(dataRDD)
		.partitionBy(new HashPartitioner(partitionSize))
		.map( c -> {

			List<SolrInputDocument> listDocs = List<SolrInputDocument>(); 

			SolrInputDocument doc = new SolrInputDocument();

			// Meta Fields
			doc.addField("id",c._1);
			
			// Archive Fields
			doc.addField("archive_active",  ((String)c._2._1.get("archive_active")).equals("1") ? true : false);
			doc.addField("archive_corpus",   c._2._1.get("archive_corpus"));
			doc.addField("archive_mime_type",c._2._1.get("archive_mime_type"));
			doc.addField("archive_country",  c._2._1.get("archive_country"));
			doc.addField("archive_lang",     c._2._1.get("archive_lang"));

			// Crawler Fields
			String[] crawl_id   = ((String)c._2._1.get("crawl_id")).split("|");
			doc.addField("crawl_id",crawl_id);
    		String[] crawl_date = Arrays.stream(crawl_session).map( cs -> { 
    			cs = cs.split("@")[1];
    			String d = cs.substring(0,4) + '-' + cs.substring(4,6) + '-' + cs.substring(6,11) + ':' + cs.substring(11,13) + ':' + cs.substring(13);
    			return d; 
    		}).toArray(size -> new String[size]);
    		doc.addField("crawl_date",      crawl_date);
    		doc.addField("crawl_date_first",crawl_date[0]);
    		doc.addField("crawl_date_last", crawl_date[crawl_date.length - 1]);	

    		// Page Fields
    		doc.addField("page_downl_id",c._1);
    		String[] page_downl_date = ((String)c._2._1.get("page_date")).split("|");
    		doc.addField("page_downl_date",      page_downl_date);
    		doc.addField("page_downl_date_first",page_downl_date[0]);
    		doc.addField("page_downl_date_last", page_downl_date[page_downl_date.length -1]);
    		String page_url = c._2._1.get("page_url");
    		doc.addField("page_domain",        Rivelaine.getDomainName(page_url));
    		doc.addField("page_url",           page_url);
    		doc.addField("page_url_id",        getShaKey(page_url));
    		doc.addField("page_space",         Rivelaine.getSiteSpace(page_url));
    		doc.addField("page_title",         c._2._2.get("page_title"));
    		doc.addField("page_description",   c._2._2.get("page_description"));
    		doc.addField("page_published_date",c._2._2.get("page_published_date"));
    		doc.addField("page_publisher",     c._2._2.get("page_publisher"));

    		// Fragment Fields
    		((List<Map<String,Object>>)c._2._2.get("fragments")).forEach(frag -> {
    			doc.addField("frag_type",  frag.get("type"));
    			doc.addField("frag_author",frag.get("author"));
    			doc.addField("frag_date",  frag.get("date"));
    			if (doc.get("frag_date") != null)
    				doc.addField("frag_date_first",frag.get("date").get(0))
    			String[] href = frag.get("href");
    			doc.addField("frag_href",href);
    			doc.addField("frag_href_id", Arrays.stream(href).map(h -> getShaKey(h)));
    			doc.addField("frag_ratio",   frag.get("ratio"));
    			doc.addField("frag_node",    frag.get("node"));
    			doc.addField("frag_offset",  frag.get("offset"));
    			doc.addField("frag_text",    frag.get("text"));
    			doc.addField("frag_text_id", getShaKey(frag.get("text")));
    			
    			// Set frag date level
    			if (doc.get("frag_date_first") != null) {
    				doc.addField("frag_date_level","1");
    			} else if (doc.get("page_published_date") != null) {
    				doc.addField("frag_date_level","2");
    			} else if (doc.get("page_downl_date_first") != null) {
    				doc.addField("frag_date_level","3");
    			} else {
    				doc.addField("frag_date_level","4");
    			}
    			
    			// Create an id / doc
    			String id =  getShaKey(frag.get("node") + (page_downl_date[0]).toString());
    			doc.addField("id",id);
    			listDocs.add(doc);
    			cleanDoc(doc);
    		});

    		return listDocs;

    	// Should be flatten
		}).flatMap(Predef.identity);

		return docs;

	}			

	public static void fragmentToSolr(
					String metaPath, 
					String dataPath, 
					int    partitionSize, 
					List<String> urlFilter, 
					String dateFrom, 
					String dateTo,
					String solrHost,
					String solrColl,
						) throws Exception{

		// Run spark job
		SparkConf conf        = new SparkConf().setMaster("local[50]").setAppName("ArchiveReader");
	    JavaSparkContext sc   = new JavaSparkContext(conf);
	    // Configuration jobConf = new Configuration();
	    // 	jobConf.setInt("mapred.max.split.size", 536870912);
	    // 	jobConf.setInt("mapred.min.split.size", 536870912);	
	    // 	jobConf.setInt("mapreduce.input.fileinputformat.split.maxsize",536870912); 	    

		System.out.println("------------[ Get MetaData ]------------");

		JavaPairRDD<BytesWritable, StreamableDAFFRecordWritable> metaData =  sc.newAPIHadoopFile(metaPath,StreamableDAFFInputFormat.class,BytesWritable.class,StreamableDAFFRecordWritable.class, jobConf);	    

		System.out.println("------------[ Process MetaDataRDD ]------------");

		JavaPairRDD<String, Map<String, Object>> metaDataRDD = getMetaDataRDD(metadata,partitionSize,urlFilter);

		System.out.println("------------[ From " + dateFrom + " To " + dateTo + " ]------------");	

    	System.out.println("------------[ " + Long.toString(metaDataRDD.count()) + " MetaData ]------------");

    	System.out.println("------------[ Create BloomFilter For Data ]------------");						

        Broadcast<List<String>> metaDataIds = sc.broadcast(metaDataRDD.keys().collect());

      	// Build a bloom filter based on metadata ids
		BloomFilter<CharSequence> siteIds = BloomFilter.create(Funnels.stringFunnel(), metaDataIds.value().size());

		for(String id : (List<String>)metaDataIds.value()) {
		  siteIds.put(id);
		}

		Broadcast<BloomFilter<CharSequence>> siteIdsBroadcast = sc.broadcast(siteIds);    

		System.out.println("------------[ Get Data ]------------");

	    JavaPairRDD<BytesWritable, StreamableDAFFRecordWritable> data =  sc.newAPIHadoopFile(dataPath,StreamableDAFFInputFormat.class,BytesWritable.class,StreamableDAFFRecordWritable.class, jobConf);

		System.out.println("------------[ Process DataRDD ]------------");	

		JavaPairRDD<String, Map<String, Object>> dataRDD = getDataRDD(data,siteIdsBroadcast,partitionSize);

		System.out.println(Long.toString(dataRDD.count()));	

		System.out.println("------------[ " + Long.toString(dataRDD.count()) + " DataRDD Processed ]------------");

		System.out.println("------------[ Get Solr Docs ]------------");

		JavaRDD<SolrInputDocument> docs = getSolrDocs(metaDataRDD,dataRDD);	

		System.out.println("------------[ Process Indexation Of " + Long.toString(docs.count()) + " Docs ]------------");

		// SolrSupport.indexDocs("lame11:2181", "ediasporas_maroco", metaSize, (RDD<SolrInputDocument>)docs.rdd());

		SolrSupport.indexDocs(solrHost, solrColl, partitionSize, (RDD<SolrInputDocument>)docs.rdd());		

	    sc.close();

	 	System.out.println("------------[ End ]------------");		    	


	}	

	public static void main(String[] args) {

		String pathToConf = args[0];

		JSONObject conf = (JSONObject)parser.parse(new FileReader(pathToConf));

		String metaPath         = (String)conf.get("metaPath");
		String dataPath         = (String)conf.get("dataPath");
		String sitePath         = (String)conf.get("sitePath");
		String solrHost         = (String)conf.get("solrHost");
		String solrColl         = (String)conf.get("solrColl");
		int    partitionSize    = (int)conf.get("partitionSize");
		List<String> urlFilter  = jsonToList((JSONArray)conf.get("urls"));
		List<String> dates      = jsonToList((JSONArray)conf.get("dates"));
		if (sitePath != null)	
			ArrayList<String> sites = fileToStringArrList(sitePath);	

		dates.stream().forEach(d -> {
			if (dates.indexOf(d) != dates.size() - 1) {
				try {
					// Go to fragmentation and archivin' dude !
					fragmentToSolr(metaPath, dataPath, partitionSize, urlFilter, d, dates.get(dates.indexOf(d) + 1), solrHost, solrColl);
				} catch(Exception e) {
					System.out.println(e.toString());
				}	
			}
		});
  	}
}