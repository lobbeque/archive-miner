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
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import java.security.MessageDigest;
import java.util.stream.Collectors;


/*
 * Scala
 */

import scala.Tuple2;
import scala.Option;

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

	/*
	 * Get the web site from the crawl session
	 */
	public static String getSite(String crawl_session) {

		return ((String[])crawl_session.split("@"))[0];

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

	public static void archiveToSolr(String metaPath, String dataPath, ArrayList<String> corpus, DateFormat df, int metaSize, ArrayList<String> urls) throws Exception{

		/*
		 * Run a local spark job with 2 threads
		 */ 

	    SparkConf conf = new SparkConf().setMaster("local[30]").setAppName("ArchiveReader");

	    // SparkConf conf = new SparkConf().setAppName("ArchiveReader");
	    
	    JavaSparkContext sc = new JavaSparkContext(conf);

	    /*
	     * Process DAFF file as a JavaPairRDD entity
	     */

	    Configuration jobConf = new Configuration();

	    jobConf.setInt("mapred.max.split.size", 536870912);
	    jobConf.setInt("mapred.min.split.size", 536870912);	
	    jobConf.setInt("mapreduce.input.fileinputformat.split.maxsize",536870912);    

        JavaPairRDD<BytesWritable, StreamableDAFFRecordWritable> metaData =  sc.newAPIHadoopFile(metaPath,StreamableDAFFInputFormat.class,BytesWritable.class,StreamableDAFFRecordWritable.class, jobConf);

		System.out.println("=====> Process MetaData");

		/*
		 * Write MetaData RDD like a JavaPairRDD<String, Map<String, String>> = < sha key, meta properties > agregated by sha key 
		 */

		JavaPairRDD<String, Map<String, Object>> metaDataRDD = metaData.filter(c -> {

				// filter first record
				Record r =	(Record)((RecordHeader)c._2.get());
				return r.content() instanceof MetadataContent ? true : false;

			}).filter(c -> {
		    	Record r = (Record)((RecordHeader)c._2.get());
		    	Map<String, Object> m = (Map)((MetadataContent)r.content()).getMetadata();	
		    	Boolean filter = false;
		    	for ( int i = 0; i < urls.size(); i ++) {
		    		if ( (((String[])((String)m.get("crawl_session")).split("@"))[0]).equals(urls.get(i)) ) {
		    			filter = true;
		    		}

		    		if ( ((String)m.get("url")).contains("http://www.yabiladi.com/forum/archive/") ) {
		    			filter = false;
		    		}
		    	}			
				return filter;
			}).mapToPair(
		  	new PairFunction<Tuple2<BytesWritable, StreamableDAFFRecordWritable>, String, Map<String, Object>>() {
		    	public Tuple2<String, Map<String, Object>> call(Tuple2<BytesWritable, StreamableDAFFRecordWritable> c) throws IOException {
		    		Record r = (Record)((RecordHeader)c._2.get());
		    		Map<String, Object> m = (Map)((MetadataContent)r.content()).getMetadata();
		     	 	return new Tuple2<String, Map<String, Object>>((String)m.get("content"), m);
		   		}
			}).reduceByKey((u,v) -> {				
				Map<String, Object> x = new HashMap<String, Object>();
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
          		x.put("type",v.get("type"));
        		x.put("url", v.get("url"));        		
        		return x;
			}).partitionBy(new HashPartitioner(metaSize));

		Broadcast<List<String>> metaDataIds = sc.broadcast(metaDataRDD.keys().collect());

        System.out.println(Long.toString(metaDataRDD.count()));

        System.out.println("=====> Building BloomFilter");

		/*
		 * Adding Bloom Filter with default expected false positive probability of 3%.
		 */

		BloomFilter<CharSequence> siteIds = BloomFilter.create(Funnels.stringFunnel(), metaDataIds.value().size());

		for(String id : (List<String>)metaDataIds.value()) {
		  siteIds.put(id);
		}

		Broadcast<BloomFilter<CharSequence>> siteIdsBroadcast = sc.broadcast(siteIds);

		/*
		 * End Bloom Filter
		 */

		System.out.println("=====> Process Data");

        JavaPairRDD<BytesWritable, StreamableDAFFRecordWritable> data =  sc.newAPIHadoopFile(dataPath,StreamableDAFFInputFormat.class,BytesWritable.class,StreamableDAFFRecordWritable.class, jobConf);

		JavaPairRDD<String, Map<String, Object>> dataRDD =	data.filter(c -> {
			Record r =	(Record)((RecordHeader)c._2.get());	
			String id = r.id();		
			return r.content() instanceof DataContent && siteIdsBroadcast.value().mightContain(id); 
		})	
		.mapToPair(
		  	new PairFunction<Tuple2<BytesWritable, StreamableDAFFRecordWritable>, String, byte[]>() {
		    	public Tuple2<String, byte[]> call(Tuple2<BytesWritable, StreamableDAFFRecordWritable> c) throws IOException {
		     	 	return new Tuple2<String, byte[]>((String)((RecordHeader)c._2.get()).id(), ((DataContent)((Record)((RecordHeader)c._2.get())).content()).get());
		   		}
		   	}
		)
		.repartition(metaSize)
		.mapValues(v -> {
			
			// new String(((DataContent)((Record)((RecordHeader)c._2.get())).content()).get())
			// byte[] content_bit = ((DataContent)r.content()).get();
			
			Map<String, Object> x = new HashMap<String, Object>();

			/*
			 * Extract links from crawled page
			 */

			Map<String, List<String>> links   = Rivelaine.getContentLink(new String(v),urls.get(0),"file");
			ArrayList<String> link_out_url    = new ArrayList<String>(links.get("out_url"));
			ArrayList<String> link_out_corpus = new ArrayList<String>();

    		corpus.forEach(s -> {
    			link_out_corpus.add("0");
    		});

    		link_out_url.stream().forEach((link) -> {
    			String domainName = Rivelaine.getDomainName(link);
				if (corpus.contains(domainName))
					link_out_corpus.set(corpus.indexOf(domainName),"1");
			});	

    		// number of out corpus links
		    // String link = link_dias_code.stream().reduce((x,y) -> x + y).get();

		    x.put("link_in_path", new ArrayList<String>());
		    x.put("link_in_url", new ArrayList<String>(links.get("in_url")));
		    x.put("link_out_social", new ArrayList<String>(links.get("out_social")));	
		    x.put("link_out_url", link_out_url);
		    x.put("link_out_corpus", link_out_corpus);	

		    /*
		     * Extract structured contents from crawled page
		     */

		    Map<String, Object> content = Rivelaine.getContentJava(new String(v),"file");	 

		    // info grabbed from the page meta    

		    x.put("page_meta_title", (String)content.get("head_title"));
		    x.put("page_meta_description", (String)content.get("head_description"));
		    x.put("page_meta_img", (String)content.get("head_img"));
		    x.put("page_meta_twitter_author", (String)content.get("head_twitter_creator"));
			x.put("page_meta_author", (String)content.get("head_publisher"));

		    if ((String)content.get("head_published_time") != "") {
		    	Date page_meta_date = Rivelaine.normalizeDate((String)content.get("head_published_time"));
		    	if (page_meta_date != null) {
		    		x.put("page_meta_date", page_meta_date);
		    	} else {
		    		// System.out.println((String)content.get("head_published_time"));
		    	}
		    }

			// info grabbed from the page content

			x.put("page_title", (String)content.get("content_title"));	
			x.put("page_author", Rivelaine.normalizeAuthor((String)content.get("content_author")));

			List<List<Map<String,Object>>> page_content = (List<List<Map<String,Object>>>)content.get("content");

		    if ((String)content.get("content_date") != "") {
		    	Date page_date = Rivelaine.normalizeDate((String)content.get("content_date"));
		    	if (page_date != null) {
		    		x.put("page_date", page_date);
		    	} else {
		    		// System.out.println((String)content.get("content_date"));
		    	}
		    }

			List<HashMap<String,Object>> listOfSeq = (List<HashMap<String,Object>>)(page_content.stream().map(seq -> {

				HashMap<String, Object> tmp = new HashMap<String, Object>(); 

				tmp.put("type",   (List<String>)(seq.stream().map(ele -> (String)ele.get("type")).collect(Collectors.toList())));
				tmp.put("mask",   (String)(seq.stream().map(ele -> (String)ele.get("type")).reduce("", String::concat)));
				tmp.put("markup", (List<String>)(seq.stream().map(ele -> (String)ele.get("markup")).collect(Collectors.toList())));
				tmp.put("depth",  (List<Integer>)(seq.stream().map(ele -> Integer.parseInt((String)ele.get("depth"))).collect(Collectors.toList())));
				tmp.put("offset", (List<Integer>)(seq.stream().map(ele -> Integer.parseInt((String)ele.get("offset"))).collect(Collectors.toList())));
				
				// Original content of the seq
				tmp.put("content", (List<String>)(seq.stream().map(ele -> {
									if ((String)ele.get("type") == "author") {
										return Rivelaine.normalizeAuthor((String)ele.get("content")); 
									} else {
										return (String)ele.get("content");
									}
								}).collect(Collectors.toList())));

				// all the text content of the seq for search
				tmp.put("text", (List<String>)(seq.stream().filter(ele -> (String)ele.get("type") != "author" && (String)ele.get("type") != "date").map(ele -> {
									return (String)ele.get("content");
								}).collect(Collectors.toList())));

				// Get all authors of the seq
				tmp.put("author", (List<String>)(seq.stream().filter(ele -> (String)ele.get("type") == "author").map(ele -> {
									return Rivelaine.normalizeAuthor((String)ele.get("content"));
								}).collect(Collectors.toList()))); 

				// Get all dates of the seq
				List<String> dates = (List<String>)(seq.stream().filter(ele -> (String)ele.get("type") == "date").map(ele -> {
					return (String)ele.get("content");
				}).collect(Collectors.toList()));

				tmp.put("date", Rivelaine.normalizeDate(dates.isEmpty() ? "" : (String)dates.get(0)));

				return tmp;
			}).collect(Collectors.toList()));

			x.put("page_content",listOfSeq);


		    return x;		
		});	

        System.out.println(Long.toString(dataRDD.count()));

		System.out.println("=====> Join Data & Meta ...");

		JavaRDD<SolrInputDocument> docs = metaDataRDD.join(dataRDD)
		.partitionBy(new HashPartitioner(metaSize))
		.map( c -> {
			
			SolrInputDocument doc = new SolrInputDocument();

			/*
			 * add archive fields
			 */

			doc.addField("id",c._1);
			doc.addField("archive_active",((String)c._2._1.get("active")).equals("1") ? true : false);
			doc.addField("archive_corpus", c._2._1.get("corpus"));
			doc.addField("archive_mime",c._2._1.get("type"));
			doc.addField("archive_ip", c._2._1.get("ip"));
    		doc.addField("archive_length", Double.parseDouble((String)c._2._1.get("length")));
    		doc.addField("archive_level", Integer.parseInt((String)c._2._1.get("level")));
    		doc.addField("archive_referer", c._2._1.get("referer_url"));

			/*
			 * add client fields
			 */

   			doc.addField("client_country", c._2._1.get("client_country"));
    		doc.addField("client_ip", c._2._1.get("client_ip"));
    		doc.addField("client_lang", ((String)c._2._1.get("client_lang")).split(", "));
    		
    		/*
    		 * add crawl & download fields
    		 */
    		
    		String[] dates = ((String)c._2._1.get("date")).split("____");

    		String[] crawl_session = ((String)c._2._1.get("crawl_session")).split("____");

    		String[] crawl_session_dates = Arrays.stream(crawl_session).map( cs -> { 
    			cs = cs.split("@")[1];
    			String d = cs.substring(0,4) + '-' + cs.substring(4,6) + '-' + cs.substring(6,11) + ':' + cs.substring(11,13) + ':' + cs.substring(13);
    			return d; 
    		}).toArray(size -> new String[size]);

    		doc.addField("download_date",dates);    		
    		doc.addField("download_date_f",dates[0]);
    		doc.addField("download_date_l",dates[dates.length - 1]);
    		
    		doc.addField("crawl_id",crawl_session);
    		doc.addField("crawl_id_f",crawl_session[0]);
    		doc.addField("crawl_id_l",crawl_session[crawl_session.length - 1]);
    		doc.addField("crawl_date",crawl_session_dates);
    		doc.addField("crawl_date_f",crawl_session_dates[0]);
			doc.addField("crawl_date_l",crawl_session_dates[crawl_session_dates.length - 1]);

			/*
			 * add page fields
			 */    		

    		doc.addField("page_site",((String[])((String)crawl_session[0]).split("@"))[0]);
    		try {
    			doc.addField("page_url_id", getShaKey((String)c._2._1.get("url")));
    		} catch(Exception e) {
    			System.out.println(e.toString());
    		}
    		doc.addField("page_url", c._2._1.get("url"));
    		doc.addField("page_space",Rivelaine.getSiteSpace((String)c._2._1.get("url")));

    		/*
    		 * add extracted page fields
    		 */ 

			doc.addField("page_link_in_path", c._2._2.get("link_in_path"));
			doc.addField("page_link_in_url", c._2._2.get("link_in_url"));
			doc.addField("page_link_out_social", c._2._2.get("link_out_social"));
			doc.addField("page_link_out_url", c._2._2.get("link_out_url"));
			doc.addField("page_link_out_corpus", c._2._2.get("link_out_corpus"));    		

    		doc.addField("page_meta_title", c._2._2.get("page_meta_title"));
    		doc.addField("page_meta_description", c._2._2.get("page_meta_description")); 
    		doc.addField("page_meta_img", c._2._2.get("page_meta_img"));
    		doc.addField("page_meta_twitter_author", c._2._2.get("page_meta_twitter_author"));
			doc.addField("page_meta_author", c._2._2.get("page_meta_author"));
			doc.addField("page_meta_date", c._2._2.get("page_meta_date"));

			doc.addField("page_title", c._2._2.get("page_title"));
			doc.addField("page_title_shingle", c._2._2.get("page_title"));
			doc.addField("page_author", c._2._2.get("page_author"));
			doc.addField("page_date", c._2._2.get("page_date"));

			/*
			 * add extracted element fields 
			 */

			if(c._2._2.get("page_content") != null) {
				((List<Map<String,Object>>)c._2._2.get("page_content")).forEach(seq -> {

					SolrInputDocument sequence = new SolrInputDocument();
					
					// create a global id for the seq such as page_id + offsets

					sequence.addField("id",c._1 + "_" +((List<Integer>)seq.get("offset")).stream().map(o -> Integer.toString(o)).reduce("", String::concat));
					
					sequence.addField("seq_type"    , (List<String>)seq.get("type"));
					sequence.addField("seq_mask"    , (String)seq.get("mask"));
					sequence.addField("seq_markup"  , (List<String>)seq.get("markup"));
					sequence.addField("seq_depth"   , (List<Integer>)seq.get("depth"));
					sequence.addField("seq_offset"  , (List<Integer>)seq.get("offset"));
					sequence.addField("seq_author"  , (List<String>)seq.get("author"));
					sequence.addField("seq_content" , (List<String>)seq.get("content"));
					sequence.addField("seq_text"    , (List<String>)seq.get("text"));

					// Select the most accurate date 

					Date seq_date;

					if (seq.get("date") != null) {
						seq_date = (Date)seq.get("date");
					} else if (c._2._2.get("page_date") != null) {
						seq_date = (Date)c._2._2.get("page_date");
					} else if (c._2._2.get("page_meta_date") != null) {
						seq_date = (Date)c._2._2.get("page_meta_date");
					} else {
						seq_date = Rivelaine.normalizeDate(dates[0]);
					}

					sequence.addField("seq_date", seq_date);					

					// seq_id = sha256(seq_content + seq_date)

					String seq_id = ((List<String>)seq.get("content")).stream().reduce("", String::concat) + seq_date.toString();

		    		try {
		    			sequence.addField("seq_id"  , getShaKey(seq_id));
		    		} catch(Exception e) {
		    			System.out.println(e.toString());
		    		}

					doc.addChildDocument(sequence);
				});
			}

			// http://lame11.enst.fr:8800/solr/ediasporas_maroco/select?q=*:*&fl=*,[child%20parentFilter=site:*]&fq=site:*

			// System.out.println(Boolean.toString(doc.hasChildDocuments()));

			return doc;
		});		

		System.out.println("=====> Start Indexing ...");	

		System.out.println(Long.toString(docs.count()));	

		SolrSupport.indexDocs("lame11:2181", "ediasporas_maroco", metaSize, (RDD<SolrInputDocument>)docs.rdd());

	    sc.close();

	    System.out.println("Done !");
	}

	public static void main(String[] args) {

		String metaPath = args[0];
		String dataPath = args[1];
		String sitePath = args[2];		
		int    metaSize = Integer.parseInt(args[3]);	

    	ArrayList<String> urls = new ArrayList<String>();

    	for ( int i = 4; i < args.length; i++ ) {
       		urls.add(args[i]);
    	}

    	System.out.println(urls.toString());			

		ObjectMapper mapper = new ObjectMapper();
			
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

		try {
			archiveToSolr(metaPath, dataPath, corpus, df, metaSize, urls);
		} catch(Exception e) {
			System.out.println(e.toString());
		}
		
  	}
}