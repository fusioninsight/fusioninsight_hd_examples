package com.huawei.bigdata.examples.hdfs.java_api;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.log4j.PropertyConfigurator;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.huawei.bigdata.security.kerberos.LoginUtil;

/**
   * HDFS创建目录、读写文件等基本操作
   */


public class BasicOperation
{
  static {
      //日志配置文件
      PropertyConfigurator.configure(BasicOperation.class.getClassLoader().getResource("conf/log4j.properties").getPath());
  }
  private final static Log LOG = LogFactory.getLog(BasicOperation.class.getName());

  private static Configuration conf = null; //HDFS配置文件
  private FileSystem fSystem; /* HDFS file system */  

  private final String DEST_PATH; //HDFS上的目标目录
  private final String FILE_NAME;

  public BasicOperation(String path, String fileName) throws IOException
  {

      this.DEST_PATH = path;
      this.FILE_NAME = fileName;
      instanceBuild();
  }

  private static void confLoad() throws IOException
  {
      conf = new Configuration();

      //加载HDFS服务端配置，包含客户端与服务端对接配置
      conf.addResource(new Path(BasicOperation.class.getClassLoader().getResource("conf/hdfs-site.xml").getPath()));
      conf.addResource(new Path(BasicOperation.class.getClassLoader().getResource("conf/core-site.xml").getPath()));        
  }

  /**
   * build HDFS instance
   */
  private void instanceBuild() throws IOException
  {
    // get filesystem
    // 一般情况下，FileSystem对象JVM里唯一，是线程安全的，这个实例可以一直用，不需要立马close。
    // 注意：
    // 若需要长期占用一个FileSystem对象的场景，可以给这个线程专门new一个FileSystem对象，但要注意资源管理，别导致泄露。
    // 在此之前，需要先给conf加上：
    // conf.setBoolean("fs.hdfs.impl.disable.cache", true);//表示重新new一个连接实例，不用缓存中的对象。
    fSystem = FileSystem.get(conf);
  }

  private static void authentication() throws IOException
  {
      //如果配置文件中指定为kerberos认证，需要登录认证。只在系统启动时执行一次
      if (!"kerberos".equalsIgnoreCase(conf.get("hadoop.security.authentication")))
      {
          return;
      }

      //认证相关，安全模式需要，普通模式可以删除
      String PRNCIPAL_NAME = "zlt";//需要修改为实际在manager添加的用户  
      String KRB5_CONF = BasicOperation.class.getClassLoader().getResource("conf/krb5.conf").getPath();
      String KEY_TAB = BasicOperation.class.getClassLoader().getResource("conf/user.keytab").getPath();
      
      System.setProperty("java.security.krb5.conf", KRB5_CONF); //指定kerberos配置文件到JVM
      LoginUtil.login(PRNCIPAL_NAME, KEY_TAB, KRB5_CONF, conf);
  }

  /**
   * 创建目录
   * @throws java.io.IOException
   */
  private void mkdir() throws IOException
  {
    LOG.info("=========== mkdir() begin ===========");

    Path destPath = new Path(DEST_PATH);
    if (!createPath(destPath))
    {
        LOG.error("failed to create destPath " + DEST_PATH);
        return;
    }

    LOG.info("success to create path " + DEST_PATH);
    LOG.info("=========== mkdir() end ===========");
  }

  /**
   * 删除目录
   * @throws java.io.IOException
   */
  private void rmdir() throws IOException
  {
      Path destPath = new Path(DEST_PATH);
      if (!deletePath(destPath))
      {
          LOG.error("failed to delete destPath " + DEST_PATH);
          return;
      }

      LOG.info("success to delete path " + DEST_PATH);

  }

  /**
   * create file,write file
   *
   * @throws java.io.IOException
   * @throws com.huawei.bigdata.hdfs.examples.ParameterException
   */
  private void write() throws IOException
  {
    LOG.info("=========== write() begin ===========");

    final String content = "hi, I am bigdata. It is successful if you can see me.";
    FSDataOutputStream out = null;
    try
    {
        out = fSystem.create(new Path(DEST_PATH + File.separator + FILE_NAME));
        out.write(content.getBytes());
        out.hsync();
        LOG.info("success to write.");
    }
    finally
    {
        // make sure the stream is closed finally.
        if (out != null)
        {
            out.close();
        }
    }

    LOG.info("=========== write() end ===========");
  }

  /**
   * append file content
   *
   * @throws java.io.IOException
   */
  private void append() throws IOException
  {
    LOG.info("=========== append() begin ===========");  

    final String content = "I append this content.";
    FSDataOutputStream out = null;
    try
    {
        out = fSystem.append(new Path(DEST_PATH + File.separator + FILE_NAME));
        out.write(content.getBytes());
        out.hsync();
        LOG.info("success to append.");
    }
    finally
    {
        // make sure the stream is closed finally.
        if (out != null)
        {
            out.close();
        }
    }

    LOG.info("=========== append() end ===========");
  }

  /**
   * read file
   *
   * @throws java.io.IOException
   */
  private void read() throws IOException
  {
    LOG.info("=========== read() begin ===========");  

    String strPath = DEST_PATH + File.separator + FILE_NAME;
    Path path = new Path(strPath);
    FSDataInputStream in = null;
    BufferedReader reader = null;
    StringBuffer strBuffer = new StringBuffer();

    try
    {
        in = fSystem.open(path);
        reader = new BufferedReader(new InputStreamReader(in));
        String sTempOneLine;

        // write file
        while ((sTempOneLine = reader.readLine()) != null)
        {
            strBuffer.append(sTempOneLine);
        }

        LOG.info("result is : " + strBuffer.toString());
        LOG.info("success to read.");

    }
    finally
    {
        // make sure the streams are closed finally.
        close(reader);
        close(in);
    }

    LOG.info("=========== read() end ===========");  
  }

  /**
   * delete file
   *
   * @throws java.io.IOException
   */
  private void delete() throws IOException
  {
    LOG.info("=========== delete() begin ===========");

    Path beDeletedPath = new Path(DEST_PATH + File.separator + FILE_NAME);
    if (fSystem.delete(beDeletedPath, true))
    {
        LOG.info("success to delete the file " + DEST_PATH + File.separator + FILE_NAME);
    }
    else
    {
        LOG.warn("failed to delete the file " + DEST_PATH + File.separator + FILE_NAME);
    }

    LOG.info("=========== delete() end ===========");
  }

  /**
   * close stream
   *
   * @param stream
   * @throws java.io.IOException
   */
  private void close(Closeable stream) throws IOException
  {
      if (stream != null)
      {
          stream.close();
      }
  }

  /**
   * create file path
   *
   * @param filePath
   * @return
   * @throws java.io.IOException
   */
  private boolean createPath(final Path filePath) throws IOException
  {
      if (!fSystem.exists(filePath))
      {
          fSystem.mkdirs(filePath);
      }
      return true;
  }

  /**
   * delete file path
   *
   * @param filePath
   * @return
   * @throws java.io.IOException
   */
  private boolean deletePath(final Path filePath) throws IOException
  {
      if (!fSystem.exists(filePath))
      {
          return false;
      }
      // fSystem.delete(filePath, true);
      return fSystem.delete(filePath, true);
  }

  public void test()  throws IOException
  {
      //创建目录
      this.mkdir();
      //写文件
      this.write();
      //追加文件
      this.append();
      //读文件
      this.read();
      //删除文件
      this.delete();
      //删除目录
      this.rmdir();
  }

  public static void main(String[] args) throws Exception
  {
    LOG.info("***************** BasicOperation Begin *****************");

    //加载配置文件
    confLoad();

    //安全模式需要进行kerberos认证，只在系统启动时执行一次。非安全模式可以删除
    //需要修改方法中的PRNCIPAL_NAME（用户名）
    authentication();  
    
    // 业务示例1：HDFS基本功能样例
    BasicOperation basicOperation = new BasicOperation("/user/hdfs-examples", "test.txt");
    basicOperation.test();

    LOG.info("***************** BasicOperation End *****************");
  }
}


