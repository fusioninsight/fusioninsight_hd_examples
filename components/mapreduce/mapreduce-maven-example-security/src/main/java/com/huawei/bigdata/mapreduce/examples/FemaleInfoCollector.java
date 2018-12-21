package com.huawei.bigdata.mapreduce.examples;

import java.io.IOException;

import com.huawei.bigdata.mapreduce.tools.LoginUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * MR实例
 */

public class FemaleInfoCollector {
	public static final String principal = "fwc@HADOOP.COM";
	public static final String keytab = FemaleInfoCollector.class.getClassLoader().getResource("user.keytab").getPath();
	public static final String krb5 = FemaleInfoCollector.class.getClassLoader().getResource("krb5.conf").getPath();

	/**
	 * main function
	 * @param args
	 *args[0]为样例MR作业输入路径，args[1]为样例MR作业输出路径
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// 初始化环境变量。
		Configuration conf = new Configuration();
		//安全登录
		LoginUtil.login(principal, keytab,krb5, conf);
		// main方法输入参数：args[0]为样例MR作业输入路径，args[1]为样例MR作业输出路径
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: FemaleInfoCollector <in> <out>");
			System.exit(2);
		}

		// 初始化job对象。
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Collect Female Info");
		//为job设置class所在的jar包
		job.setJarByClass(FemaleInfoCollector.class);

		//设置map和reduce所执行的类,也可以在“mapred-site.xml”中配置“mapreduce.job.map.class”/“mapreduce.job.reduce.class”项。
		job.setMapperClass(CollectionMapper.class);
		job.setReducerClass(CollectionReducer.class);

		//设置Combiner类。 Combiner类是用于提高MapReduce的性能，作用在Map与Reduce之间，合并减少Mapper的输出，减轻Reduce的压力
		//在写Combine时，必须保证即使没有Combine也能运行，因此最好先写mapper和reduce，之后再添加Combine类。
		//combiner类，默认不使用，使用时通常使用和reduce一样的类，Combiner类需要谨慎使用。
		job.setCombinerClass(CollectionCombiner.class);

		// 设置job的输出类型。
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// 将job提交到远程环境以执行。 true表示将运行进度等信息及时输出给用户，false的话只是等待作业结束
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	/**
	 * Mapper 类
	 * 作用：过滤原文件中的女性数据，并把姓名和网购时长 作为key-value输出
	 */
	public static class CollectionMapper extends Mapper<Object, Text, Text, IntWritable> {

		// 分隔符
		String delim;
		// 过滤性别
		String sexFilter;
		// 名字
		private Text nameInfo = new Text();
		// 输出<key，value>必须序列化。
		private IntWritable timeInfo = new IntWritable(1);
		/**
		 * 分布式计算
		 * @param key
		 *            Object : 源文件的位置偏移量
		 * @param value
		 *            Text : 源文件中的一行字符
		 * @param context
		 *            Context : 输出参数
		 * @throws IOException
		 *             , InterruptedException
		 */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			if (line.contains(sexFilter)) {

				// 获取 姓名
				String name = line.substring(0, line.indexOf(delim));
				nameInfo.set(name);
				// 获取 网购停留时间
				String time = line.substring(line.lastIndexOf(delim) + 1, line.length());
				timeInfo.set(Integer.parseInt(time));

				// Map任务输出一个键值对。
				context.write(nameInfo, timeInfo);
			}
		}

		/**
		 * 在map（）方法,或reduce（）之前只调用一次setup（）方法，用于初始化
		 * @param context
		 *            Context
		 */
		public void setup(Context context) throws IOException, InterruptedException {
			//使用Context获取配置信息。 获取分隔符 和 性别过滤条件
			delim = context.getConfiguration().get("log.delimiter", ",");
			sexFilter = delim + context.getConfiguration().get("log.sex.filter", "female") + delim;
		}
	}

	/**
	 * Reducer 类
	 * 作用：统计相同姓名的女性所有网购时间总和，并将满足网购总时长超过120分钟的 姓名和结果以key-value输出
	 */
	public static class CollectionReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// 统计结果
		private IntWritable result = new IntWritable();
		// 总时间阈值
		private int timeThreshold;

		/**
		 * @param key
		 *            Text : Mapper输出的key
		 * @param values
		 *            Iterable : map任务后具有相同key的所有结果
		 * @param context
		 *            Context
		 * @throws IOException
		 *             , InterruptedException
		 */
		//输入为一个key和value值集合迭代器。由各个map汇总相同的key而来。
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			//如果时间小于阈值，则不输出结果。
			if (sum < timeThreshold) {
				return;
			}
			result.set(sum);

			//在输出信息中，key表示网民信息，result表示网民的在线总时间。
			context.write(key, result);
		}

		/**
		 * 在map（）方法,或reduce（）之前只调用一次setup（）方法，用于初始化
		 * @param context
		 *            Context
		 * @throws IOException
		 *             , InterruptedException
		 */
		public void setup(Context context) throws IOException, InterruptedException {
			// 获取 网购总时间阈值
			timeThreshold = context.getConfiguration().getInt("log.time.threshold", 120);
		}
	}

	/**
	 * Combiner 类 。在mapper后，reducer之前做 中间阶段的结果统计，合并减少Mapper的输出，减轻Reduce的压力。
	 */
	public static class CollectionCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable intermediateResult = new IntWritable();

		/**
		 * @param key
		 *            Text : Mapper处理后的key
		 * @param values
		 *            Iterable : map任务后具有相同key的所有结果
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

			//在输出信息中，key表示网民信息，result表示网民的在线总时间。
			context.write(key, intermediateResult);
		}
	}
}
