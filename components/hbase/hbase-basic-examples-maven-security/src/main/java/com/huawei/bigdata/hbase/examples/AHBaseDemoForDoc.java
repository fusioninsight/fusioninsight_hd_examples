package com.huawei.bigdata.hbase.examples;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.User;
import org.apache.log4j.PropertyConfigurator;
import com.huawei.bigdata.security.LoginUtil;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 用于配合文档说明HBase开发核心处理，更多HBase操作请参考其他样例代码
 * 代码可以直接在IDE工程中执行。
 *
 * 场景说明：
 *- 创建一个表。
 *- 给表中添加数据。
 *- 修改表，扩展列族。
 *- 根据rowkey查询一条数据。
 *- 根据rowkey删除一条数据。
 *- 扫描获取表中存在的所有数据数据。
 *- 根据条件，扫描获取表中满足条件数据。
 *- 删除表。
 * @author fwx619776
 */
public class AHBaseDemoForDoc {
  private static final Log LOG = LogFactory.getLog(AHBaseDemoForDoc.class.getName());
  private static final String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop.hadoop.com";

  private static TableName tableName = null;
  private static Connection conn = null;
  private static Admin admin = null;
  private static Table table = null;

  static {
    //日志配置文件
    PropertyConfigurator.configure(AHBaseDemoForDoc.class.getClassLoader().getResource("conf/log4j.properties").getPath());
  }

  public static void main(String[] args)
  {

    //创建配置文件对象
    Configuration conf = HBaseConfiguration.create();

    //加载HDFS/HBase服务端配置，用于客户端与服务端对接
    conf.addResource(new Path(AHBaseDemoForDoc.class.getClassLoader().getResource("conf/core-site.xml").getPath()), false);
    conf.addResource(new Path(AHBaseDemoForDoc.class.getClassLoader().getResource("conf/hdfs-site.xml").getPath()), false);
    conf.addResource(new Path(AHBaseDemoForDoc.class.getClassLoader().getResource("conf/hbase-site.xml").getPath()), false);

    try
    {
      //安全登录,请根据实际情况：安全模式需要安全登录；普通模式不需要
      if (User.isHBaseSecurityEnabled(conf))
      {
        String userName = "lyysxg";//请根据实际情况，修改“TestUser”为实际用户名
        String userKeytabFile = AHBaseDemoForDoc.class.getClassLoader().getResource("conf/user.keytab").getPath();
        String krb5File = AHBaseDemoForDoc.class.getClassLoader().getResource("conf/krb5.conf").getPath();

        //配置ZooKeeper认证信息。ZooKeeper为HBase集群中各进程提供分布式协作服务
        LoginUtil.setJaasFile(userName,userKeytabFile);
        LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
        LoginUtil.login(userName, userKeytabFile, krb5File, conf);
      }
    }
    catch (IOException e)
    {
      LOG.error("Failed to login because ", e);
      return;
    }

    try
    {
      //创建Connection对象,用于连接 HBase服务器 和 ZooKeeper，Connection也提供实例化Admin和Table对象的方法。
      conn = ConnectionFactory.createConnection(conf);
    }
    catch (Exception e)
    {
      LOG.error("Failed to createConnection because ", e);
    }


    LOG.info("-----------Entering HBase test-------------------");
    try
    {
      //******  创建一个表。  ******
      tableName = TableName.valueOf("hbase_table_demo");//修改“hbase_demo_table”为实际表名
      //传入表名tableName，实例化一个表的描述对象  tableName："hbase_table_demo"
      HTableDescriptor htd = new HTableDescriptor(tableName);
      //设置一个列族名为“info”。
      HColumnDescriptor hcd = new HColumnDescriptor("info");
      //设置进行compaction的时候文件压缩方式，HBase默认提供了GZ和SNAPPY两种压缩算法
      //其中GZ的压缩率高，但压缩和解压性能低，适用于冷数据
      //SYNAPP压缩率低，但压缩解压性能高，适用于热数据
      //建议默认开启SNAPPY压缩,目标是达到尽可能快的压缩和解压速度，同时减少对CPU的消耗。
      hcd.setCompressionType(Compression.Algorithm.SNAPPY);
      //设置编码算法，HBase提供了DIFF，FAST_DIFF，PREFIX和PREFIX_TREE四种编码算法
      //snappy+prefix_tree在性能上基本没有提升，CPU开销却上升,不建议同时开启snappy压缩和prefix_tree编码
      //一般键比较长（与值相比）或有许多列，请使用前缀编码器。建议使用FAST_DIFF。
      hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
      htd.addFamily(hcd);

      // 实例化Admin对象。Admin提供了建表、创建列族、检查表是否存在、修改表结构和列族结构以及删除表等功能，以及执行其他管理操作
      admin = conn.getAdmin();
      if (!admin.tableExists(tableName))
      {
        LOG.info("Creating table...");
        // 创建一个预划分region的表
        // 方式1：通过指定起始和结束RowKey 和region 个数 admin.createTable(hcd, Bytes.toBytes(10), Bytes.toBytes(800000), 30);
        // 方式2：通过RowKey数组 预分Region
        byte[][] splits = new byte[4][];
        splits[0] = Bytes.toBytes("A");
        splits[1] = Bytes.toBytes("H");
        splits[2] = Bytes.toBytes("O");
        splits[3] = Bytes.toBytes("U");
        admin.createTable(htd, splits);
        LOG.info(admin.getClusterStatus());
        LOG.info(admin.listNamespaceDescriptors());
        LOG.info("Table created successfully.");
      }
      else
      {
        LOG.warn("table already exists");
      }

      //******  给表中添加数据。  ******
      // HBase通过HTable的put方法来Put数据，可以是一行数据put(Put p)也可以是数据集put(List<Put> ps)。
      //初始化一个Table对象，用于对表进行get, put, delete or scan操作（这些方法需以Get, Put, Delete or Scan对象作为参数传入）。
      table = conn.getTable(tableName);

      List<Put> puts = new ArrayList<Put>();
      // 设置列族名称为"info" ， 列名为"name"，"gender"，"age"，"address"
      byte[] familyName = Bytes.toBytes("info");
      byte[][] qualifiers = { Bytes.toBytes("name"), Bytes.toBytes("gender"), Bytes.toBytes("age"),
              Bytes.toBytes("address") };

      //实例化Put对象。
      //HBase是以RowKey为字典排序的分布式数据库系统，RowKey的设计对性能影响很大。
      //大家开发时请结合具体的业务来设计RowKey。
      Put put = new Put(Bytes.toBytes("Z0001"));
      put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Zhang San"));
      put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Male"));
      put.addColumn(familyName, qualifiers[2], Bytes.toBytes("19"));
      put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Shenzhen, Guangdong"));
      puts.add(put);

      put = new Put(Bytes.toBytes("L0002"));
      put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Li Wanting"));
      put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Female"));
      put.addColumn(familyName, qualifiers[2], Bytes.toBytes("23"));
      put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Shijiazhuang, Hebei"));
      puts.add(put);

      put = new Put(Bytes.toBytes("W0003"));
      put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Wang Ming"));
      put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Male"));
      put.addColumn(familyName, qualifiers[2], Bytes.toBytes("26"));
      put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Ningbo, Zhejiang"));
      puts.add(put);


      // 提交put请求。可以是一行数据也可以是数据集
      table.put(puts);
      LOG.info("Put successfully.");


      //******  修改表  扩展列族。******
      LOG.info("Entering testModifyTable.");
      // 设置新一个列族名称为"education"
      byte[] newFamilyName = Bytes.toBytes("education");
      htd = admin.getTableDescriptor(tableName);
      // 添加新列族前检查表中是否已经存在该列族
      if (!htd.hasFamily(newFamilyName))
      {
        hcd = new HColumnDescriptor(newFamilyName);
        htd.addFamily(hcd);

        // 先disableTable，因为修改表需要在表禁用的状态下才能生效。
        admin.disableTable(tableName);
        // 提交modifyTable请求。
        admin.modifyTable(tableName, htd);
        // 修改表后，启用表以。
        admin.enableTable(tableName);
      }
      LOG.info("Modify table successfully.");


      //******  使用Get读取数据  ******
      LOG.info("Entering testGet.");
      byte[] rowKey = Bytes.toBytes("W0003");
      // 使用rowkey实例化Get对象.
      Get get = new Get(rowKey);
      // 给get对象添加查询的列族和列名信息.
      byte[] familyNameForGet = Bytes.toBytes("info");
      byte[][] qualifier = { Bytes.toBytes("name"), Bytes.toBytes("address") };
      get.addColumn(familyNameForGet, qualifier[0]);
      get.addColumn(familyNameForGet, qualifier[1]);
      // 提交get 请求.
      //Result为获取或扫描查询的单行结果。
      //根据多个Rowkey获取多条数据可以使用table的get(List<Get> var1) 返回Result[]，用法同前面table.put(puts);
      Result result = table.get(get);
      // 打印请求结果.
      for (Cell cell : result.rawCells())
      {
        LOG.info(Bytes.toString(CellUtil.cloneRow(cell)) + ":" + Bytes.toString(CellUtil.cloneFamily(cell))
                + "," + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
                + Bytes.toString(CellUtil.cloneValue(cell)));
      }
      LOG.info("Get data successfully.");



      //****** 删除表中数据  ******
      LOG.info("Entering testDelete.");
      // 设置要删除数据的RowKey为"Z0001"
      byte[] rowKeyForDelete = Bytes.toBytes("Z0001");
      // 用要删除的rowkey,实例化Delete对象.
      Delete delete = new Delete(rowKeyForDelete);
      // 提交delete 请求.
      table.delete(delete);
      LOG.info("Delete table successfully.");


      //******  使用Scan读取数据  ******
      LOG.info("Entering testScanData.");
      //实例化ResultScanner对象,ResultScanner类把扫描操作转换为类似的get操作，它将每一行数据封装成一个Result实例，
      //并将所有的Result实例放入一个迭代器中。next()调用返回了一个单独的Result实例。
      ResultScanner rScanner = null;
      Scan scan = new Scan();
      //建议Scan时指定StartRow和StopRow，一个有确切范围的Scan，性能会更好些。
      //  scan.setStartRow(Bytes.toBytes("Z0001"));
      //  scan.setStopRow(Bytes.toBytes("W0003"));
      scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
      //设置扫描的缓存行数。
      scan.setCaching(1000);

      // 提交scan 请求.
      rScanner = table.getScanner(scan);
      // 打印请求结果.
      for (Result r = rScanner.next(); r != null; r = rScanner.next())
      {
        for (Cell cell : r.rawCells())
        {
          LOG.info(Bytes.toString(CellUtil.cloneRow(cell)) + ":" + Bytes.toString(CellUtil.cloneFamily(cell))
                  + "," + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
                  + Bytes.toString(CellUtil.cloneValue(cell)));
        }
      }
      LOG.info("Scan data successfully.");


      //******  使用过滤器Filter, 用名字信息作为过滤条件   ******
      // Filter主要在Scan和Get过程中进行数据过滤。
      // 过滤器在客户端创建，通过RPC传送到服务器端，然后在服务器端进行过滤操作，将符合条件的数据返回客户端
      // 从而减少从region服务器向客户端发送的数据，从而减少数据传输，提高效率。
      //比较过滤器:
      // 1、行键过滤器 RowFilter 2、列簇过滤器 FamilyFilter 3、列过滤器 QualifierFilter
      // 4、值过滤器 ValueFilter 5、时间戳过滤器 TimestampsFilter
      //专用过滤器:
      //1、单列值过滤器 SingleColumnValueFilter ----会返回满足条件的整行
      //2、单列值排除器 SingleColumnValueExcludeFilter
      //3、前缀过滤器 PrefixFilter----针对行键
      //4、列前缀过滤器 ColumnPrefixFilter
      //5、分页过滤器 PageFilter

      SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"),
              CompareOp.EQUAL, Bytes.toBytes("Wang Ming"));

      scan.setFilter(filter);
      rScanner = table.getScanner(scan);
      // 打印请求结果.
      for (Result r = rScanner.next(); r != null; r = rScanner.next())
      {
        for (Cell cell : r.rawCells())
        {
          LOG.info(Bytes.toString(CellUtil.cloneRow(cell)) + ":" + Bytes.toString(CellUtil.cloneFamily(cell))
                  + "," + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
                  + Bytes.toString(CellUtil.cloneValue(cell)));
        }
      }
      if (rScanner != null)
      {
        rScanner.close();
      }
      LOG.info("Single column value filter successfully.");


      //******  删除表   ******
      LOG.info("Entering dropTable.");
      if (admin.tableExists(tableName))
      {
        // 先禁用表，因为删除表要在表禁用的状态下才能生效。
        admin.disableTable(tableName);
        // 删除表。
        admin.deleteTable(tableName);
      }
      LOG.info("Drop table successfully.");


    }
    catch (IOException e)
    {
      LOG.error("HBase test failed.", e);
    }
    catch (Exception e)
    {
      LOG.error("HBase test failed.", e);
    }
    finally
    {
      if (table != null)
      {
        try
        {
          // 关闭table对象
          table.close();
        }
        catch (IOException e)
        {
          LOG.error("Failed to close table ", e);
        }
      }
      if (admin != null)
      {
        try
        {
          // 关闭Admin对象。
          admin.close();
        }
        catch (IOException e)
        {
          LOG.error("Failed to close admin ", e);
        }
      }
      if (conn != null)
      {
        try
        {
          // 关闭Connection对象。
          conn.close();
        }
        catch (IOException e)
        {
          LOG.error("Failed to close the Connection ", e);
        }
      }
    }

    LOG.info("Exiting test.");
    LOG.info("-----------finish HBase -------------------");
  }
}
