package com.huawei.bigdata.mapreduce.examples;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.impl.CloudSolrClient;

import com.huawei.bigdata.examples.util.JarFinderUtil;
import com.huawei.fusioninsight.solr.example.PeopleInfo;
import com.huawei.fusioninsight.solr.example.SolrException;
import com.huawei.fusioninsight.solr.example.SolrUtil;

public class MapReduceToSolr {

	private static final Log LOG = LogFactory.getLog(MapReduceToSolr.class);

	// security info
	private static final String ConfPath = "/opt/zhuojunjian/conf/";
	//please put these files in one path and export then with a command like "export YARN_USER_CLASSPATH=...."
	private static final String KEYTAB = ConfPath + "user.keytab";
	private static final String KRB = ConfPath + "krb5.conf";
	private static final String JAAS = ConfPath + "jaas_mr.conf";

	/**
	 * Mapper class
	 */
	public static class CollectionMapper extends Mapper<Object, Text, Text, IntWritable> {

		// Delimiter
		String delim;

		// Filter sex.
		String sexFilter;

		// Name
		private Text nameInfo = new Text();

		// Output <key,value> must be serialized.
		private IntWritable timeInfo = new IntWritable(1);

		/**
		 * Distributed computing
		 *
		 * @param key
		 *            Object : location offset of the source file
		 * @param value
		 *            Text : a row of characters in the source file
		 * @param context
		 *            Context : output parameter
		 * @throws IOException
		 *             , InterruptedException
		 */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();

			if (line.contains(sexFilter)) {

				// A character string that has been read
				String name = line.substring(0, line.indexOf(delim));
				nameInfo.set(name);
				// Obtain the dwell duration.
				String time = line.substring(line.lastIndexOf(delim) + 1, line.length());
				timeInfo.set(Integer.parseInt(time));

				// The Map task outputs a key-value pair.
				context.write(nameInfo, timeInfo);
			}
		}

		/**
		 * map use to init
		 *
		 * @param context
		 *            Context
		 */
		public void setup(Context context) throws IOException, InterruptedException {

			// Obtain configuration information using Context.
			delim = context.getConfiguration().get("log.delimiter", ",");

			sexFilter = delim + context.getConfiguration().get("log.sex.filter", "female") + delim;
		}

	}

	/**
	 * Reducer class
	 */
	public static class CollectionReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private static final Logger LOG = Logger.getLogger(CollectionReducer.class);
		// Statistical results
		private IntWritable result = new IntWritable();

		// Total time threshold
		private int timeThreshold;

		private CloudSolrClient cloudSolrClient;
		private static final String ZK_URL = "189.132.190.106:24002,189.132.190.171:24002,189.132.64.127:24002/solr";

		/**
		 * @param key
		 *            Text : key after Mapper
		 * @param values
		 *            Iterable : all statistical results with the same key
		 * @param context
		 *            Context
		 * @throws IOException
		 *             , InterruptedException
		 */
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}

			// No results are output if the time is less than the threshold.
			if (sum < timeThreshold) {
				return;
			}
			result.set(sum);

			// TODO : added by zhuojunjian
			try {
				SolrUtil.addDocs(cloudSolrClient, new PeopleInfo(new String(key.copyBytes()), "female", sum));
			} catch (SolrException e) {
				LOG.error("failed to add solr doc, because:" + e);
				throw new InterruptedException(e.getMessage());
			}

			// In the output information, key indicates netizen information, and
			// value indicates the total online time of the netizen.
			context.write(key, result);
		}

		/**
		 * The setup() method is invoked for only once before the map() method
		 * or reduce() method.
		 *
		 * @param context
		 *            Context
		 * @throws IOException
		 *             , InterruptedException
		 */
		public void setup(Context context) throws IOException, InterruptedException {
			// for components that depend on Zookeeper, need provide the conf of
			// jaas and krb5
			// Notice, no need to login again here, will use the credentials in
			// main function
			String krb5 = "krb5.conf";
			String jaas = "jaas_mr.conf";
			// These files are uploaded at main function
			File jaasFile = new File(jaas);
			File krb5File = new File(krb5);
			System.setProperty("java.security.auth.login.config", jaasFile.getCanonicalPath());
			System.setProperty("java.security.krb5.conf", krb5File.getCanonicalPath());
			System.setProperty("zookeeper.sasl.client", "true");
			//becare of dealm name. 
			System.setProperty("zookeeper.server.principal", "zookeeper/hadoop.hadoop.com");

			try {
				cloudSolrClient = SolrUtil.getCloudSolrClient(ZK_URL);
				cloudSolrClient.setDefaultCollection("col_test");
			} catch (SolrException e) {
				LOG.error("failed to add solr doc, because:" + e);
				throw new InterruptedException(e.getMessage());
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			cloudSolrClient.close();
		}

	}

	/**
	 * Combiner class
	 */
	public static class CollectionCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

		// Intermediate statistical results
		private IntWritable intermediateResult = new IntWritable();

		/**
		 * @param key
		 *            Text : key after Mapper
		 * @param values
		 *            Iterable : all results with the same key in this map task
		 * @param context
		 *            Context
		 * @throws IOException
		 *             , InterruptedException
		 */
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}

			intermediateResult.set(sum);

			// In the output information, key indicates netizen information,
			// and value indicates the total online time of the netizen in this
			// map task.
			context.write(key, intermediateResult);
		}

	}

	/**
	 * main function
	 *
	 * @param args
	 *            String[] : index 0:process file directory index 1:process out
	 *            file directory
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String files = "file://" + KEYTAB + "," + "file://" + KRB + "," + "file://" + JAAS;

		// this setting will ask Job upload these files to HDFS
		conf.set("tmpfiles", files);

		// find dependency jars for solr
		Class solrClient = Class.forName("org.apache.solr.client.solrj.impl.CloudSolrClient");
		Class solrServer = Class.forName("org.apache.solr.util.CryptoKeys");
		Class solrJson = Class.forName("org.noggit.JSONWriter");
		Class solrHTTPMime = Class.forName("org.apache.http.entity.mime.content.ContentBody");

		// add dependency jars to Job
		JarFinderUtil.addDependencyJars(conf, solrClient, solrServer, solrJson, solrHTTPMime);
		// JarFinderUtil.addDependencyJars(conf, solrClient, solrServer,
		// solrJson);

		// Initialize the job object.
		Job job = Job.getInstance(conf);
		job.setJarByClass(MapReduceToSolr.class);

		// set mapper&reducer class
		job.setMapperClass(CollectionMapper.class);
		job.setReducerClass(CollectionReducer.class);

		// set job input&output
		String inputPath = "/user/tester1/mapreduce/input";
		String outputPath = "/user/tester1/mapreduce/output";
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		// Set the output type of the job.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Submit the job to a remote environment for execution.
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
