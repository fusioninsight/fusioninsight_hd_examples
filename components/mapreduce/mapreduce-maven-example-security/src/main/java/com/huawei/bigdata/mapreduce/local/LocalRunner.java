package com.huawei.bigdata.mapreduce.local;

import java.io.File;

import com.huawei.bigdata.mapreduce.tools.FileUploader;
import com.huawei.bigdata.mapreduce.tools.LoginUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.huawei.bigdata.mapreduce.examples.FemaleInfoCollector;
import com.huawei.bigdata.mapreduce.examples.FemaleInfoCollector.CollectionCombiner;
import com.huawei.bigdata.mapreduce.examples.FemaleInfoCollector.CollectionMapper;
import com.huawei.bigdata.mapreduce.examples.FemaleInfoCollector.CollectionReducer;
import com.huawei.bigdata.mapreduce.tools.TarManager;
import org.apache.log4j.PropertyConfigurator;

public class LocalRunner {

	static {
		//日志配置文件
		PropertyConfigurator.configure(LocalRunner.class.getClassLoader().getResource("log4j.properties").getPath());
	}

    /**
     *首先生成JAR，使用CreateJar方法创建JAR
     * @throws Exception
     */
	public static void main(String[] args) throws Exception {

		//创建jar包。后面有使用到。
		TarManager.createJar();

		// 初始化环境变量，加载连接服务端的 xml 配置文件
		Configuration conf = new Configuration();
        conf.addResource(LocalRunner.class.getClassLoader().getResourceAsStream("core-site.xml"));
        conf.addResource(LocalRunner.class.getClassLoader().getResourceAsStream("yarn-site.xml"));
        conf.addResource(LocalRunner.class.getClassLoader().getResourceAsStream("mapred-site.xml"));
        conf.addResource(LocalRunner.class.getClassLoader().getResourceAsStream("hdfs-site.xml"));
        conf.addResource(LocalRunner.class.getClassLoader().getResourceAsStream("user-mapred.xml"));

        String principal = "fwc@HADOOP.COM";
        String keytab = LocalRunner.class.getClassLoader().getResource("user.keytab").getPath();
        String krb5 = LocalRunner.class.getClassLoader().getResource("krb5.conf").getPath();
        //安全登录
		LoginUtil.login(principal, keytab,krb5, conf);

		// 从xml配置文件中获取 本机源文件名、MR输入和MR输出路径
		String filelist = conf.get("username.client.filelist.conf");
		String inputPath = conf.get("username.client.mapred.input");
		String outputPath = conf.get("username.client.mapred.output");

		//获取当前路径
		String dir = System.getProperty("user.dir");
		//源文件所在的本地目录
		String localPath = dir + File.separator + "conf";
		// 将本地源文件上传到集群hdfs inputPath目录
		FileSystem fileSystem = FileSystem.get(conf);
		putFiles(fileSystem, filelist, localPath, inputPath);


		//删除输出路径，保证输出路径不存在，否则会报错
		if (fileSystem.exists(new Path(outputPath))) {
			fileSystem.delete(new Path(outputPath), true);
		}

		// 初始化Job对象。
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Collect Female Info");

		//设置执行jar和class， 通过传入的class 找到job的jar包
		job.setJar(dir + File.separator + "mapreduce-examples.jar");
		job.setJarByClass(FemaleInfoCollector.class);

        //设置map和reduce所执行的类,也可以在“mapred-site.xml”中配置“mapreduce.job.map.class”/“mapreduce.job.reduce.class”项。
		job.setMapperClass(CollectionMapper.class);
		job.setReducerClass(CollectionReducer.class);

        //设置Combiner类。 Combiner类是用于提高MapReduce的性能，作用在Map与Reduce之间，合并减少Mapper的输出，减轻Reduce的压力
        //在写Combine时，必须保证即使没有Combine也能运行，因此最好先写mapper和reduce，之后再添加Combine类。
        //combiner类，默认不使用，使用时通常使用和reduce一样的类，Combiner类需要谨慎使用。
        job.setCombinerClass(CollectionCombiner.class);

		//设置job的输出类型。
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 设置hdfs上的输入输出路径
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		//将job提交到远程环境以执行。 true表示将运行进度等信息及时输出给用户，false的话只是等待作业结束
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}


    /**
     *将本地文件上传到远程文件系统
     * @param fileSystem
     * FileSystem：文件系统
     * @param fileConfig
     * String：要上传的conf文件
     * @param localPath
     *字符串：本地文件目录
     * @param inputPath
     *字符串：远程目标路径
     * @return boolean：结果
	 */
	private static void putFiles(FileSystem fileSystem, final String fileConfig, final String localPath,
			final String inputPath) throws Exception {

		// local files which are to be uploaded
		String[] filenames = fileConfig.split(",");

		if (filenames == null || filenames.length <= 0) {
			throw new Exception("The files to be uploaded are not specified.");
		}

		// file loader to hdfs
		FileUploader fileLoader = null;

		for (int i = 0; i < filenames.length; i++) {
			if (filenames[i] == null || "".equals(filenames[i])) {
				continue;
			}

			// Excute upload hdfs
			fileLoader = new FileUploader(fileSystem, inputPath, filenames[i],
					localPath + File.separator + filenames[i]);
			fileLoader.upload();
		}
	}
}
