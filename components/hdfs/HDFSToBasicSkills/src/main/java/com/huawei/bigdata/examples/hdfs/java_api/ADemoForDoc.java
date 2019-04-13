/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.huawei.bigdata.examples.hdfs.java_api;

import com.huawei.bigdata.examples.hdfs.security.LoginUtil;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.PropertyConfigurator;

/**
 * 用于配合文档说明HDFS开发核心处理，更多HDFS操作请参考其他样例代码
 * @author z00369970
 */
public class ADemoForDoc {

  static {
    //日志配置文件
    PropertyConfigurator.configure(ADemoForDoc.class.getClassLoader().getResource("conf/log4j.properties").getPath());
  }
  private final static Log LOG = LogFactory.getLog(ADemoForDoc.class.getName());

  private static void close(Closeable stream) throws IOException
  {
    if (stream != null)
    {
      stream.close();
    }
  }

  public static void main(String[] args) throws Exception
  {
    final String DIR_PATH = "/user/hdfs-examples"; //HDFS上的目标目录
    final String FILE_PATH = DIR_PATH + File.separator + "test.txt";
      LoginUtil.login();
    //根据配置文件创建FileSystem对象，FileSystem是hadoop定义的一个通用文件系统的接口类
    //这是个抽象类，只能通过静态工厂方法get到。
    Configuration conf = new Configuration();
    FileSystem fSystem = FileSystem.get(conf);

    //创建目录
    Path dirPath = new Path(DIR_PATH);

    //清理环境，如果当前目录已经存在，先删除
    if (fSystem.exists(dirPath))
    {
      fSystem.delete(dirPath, true);//第二个参数用于指定是否递归删除
    }

    //******创建目录********
    fSystem.mkdirs(dirPath);
    LOG.info("success to mkdirs.");

    //******创建文件，并写入内容******
    FSDataOutputStream out = null;
    String content = "hi, I am bigdata. It is successful if you can see me.";

    try
    {
      //创建文件输出流
      out = fSystem.create(new Path(FILE_PATH));
      out.write(content.getBytes());
      out.hsync();//保证数据被DataNode持久化
      LOG.info("success to write.");
    }
    finally
    {
      //写入完成后关闭数据流.
      close(out);
    }

    //******读取文件******
    FSDataInputStream in = null;
    BufferedReader reader = null;
    StringBuilder strBuffer = new StringBuilder();

    try
    {
      //创建文件读取流
      in = fSystem.open(new Path(FILE_PATH));

      //缓冲文件读取流
      reader = new BufferedReader(new InputStreamReader(in));
      String sTempOneLine;

      //读取文件数据
      while ((sTempOneLine = reader.readLine()) != null)
      {
        strBuffer.append(sTempOneLine);
      }

      LOG.info("result is : " + strBuffer.toString());
      LOG.info("success to read.");
    }
    finally
    {
      //确保关闭已开启的数据流
      close(reader);
      close(in);
    }

    //清理环境，删除目录和文件
    fSystem.delete(dirPath, true);
    LOG.info("success to delete.");
  }
}
