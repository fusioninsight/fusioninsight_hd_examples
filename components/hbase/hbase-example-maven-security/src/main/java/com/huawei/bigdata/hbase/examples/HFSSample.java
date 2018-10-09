/*
 * Copyright Notice:
 *      Copyright  1998-2009, Huawei Technologies Co., Ltd.  ALL Rights Reserved.
 *
 *      Warning: This computer software sourcecode is protected by copyright law
 *      and international treaties. Unauthorized reproduction or distribution
 *      of this sourcecode, or any portion of it, may result in severe civil and
 *      criminal penalties, and will be prosecuted to the maximum extent
 *      possible under the law.
 */
package com.huawei.bigdata.hbase.examples;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.filestream.client.FSFile;
import org.apache.hadoop.hbase.filestream.client.FSGet;
import org.apache.hadoop.hbase.filestream.client.FSHColumnDescriptor;
import org.apache.hadoop.hbase.filestream.client.FSPut;
import org.apache.hadoop.hbase.filestream.client.FSResult;
import org.apache.hadoop.hbase.filestream.client.FSTable;
import org.apache.hadoop.hbase.filestream.client.FSTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

public class HFSSample
{
    public static final Log LOG = LogFactory.getLog(HFSSample.class);

    private static final String INPUTFILE = System.getProperty("user.dir") + File.separator + "conf" + File.separator
            + "inputfile.txt";

    private TableName tableName = null;

    private Configuration conf = null;

    private Connection conn = null;

    public HFSSample(Configuration conf) throws IOException
    {
        this.conf = conf;
        this.tableName = TableName.valueOf("HFS_TABLE");
        this.conn = ConnectionFactory.createConnection(conf);
    }

    public void test() throws Exception
    {
        try
        {
            createTable();
            putFiles();
            getFiles();
            cleanup();
        }
        catch (Exception e)
        {
            throw e;
        }
        finally
        {
            if (conn != null)
            {
                try
                {
                    conn.close();
                }
                catch (Exception e1)
                {
                    LOG.error("Failed to close the connection ", e1);
                }
            }
        }
    }

    /**
     * Create HFS table.
     */
    public void createTable() throws IOException
    {
        try (Admin admin = conn.getAdmin())
        {
            LOG.info("Start to create table.");
            FSTableDescriptor tableDescriptor = new FSTableDescriptor(tableName);
            HColumnDescriptor normalCd = new HColumnDescriptor("I");
            FSHColumnDescriptor largeCd = new FSHColumnDescriptor(Bytes.toBytes("F"));
            largeCd.setFileColumn();
            largeCd.setFileThreshold(5 * 1024 * 1024);

            tableDescriptor.addFamily(normalCd);
            tableDescriptor.addFamily(largeCd);

            admin.createTable(tableDescriptor);
            LOG.info("Create table successfully.");
        }
        catch (IOException e)
        {
            throw new IOException("Create table failed!", e);
        }
    }

    /**
     * Put files to HBase.
     */
    public void putFiles() throws IOException
    {
        FSTable fsTable = null;
        try
        {
            LOG.info("Start to put file to HBase.");
            fsTable = new FSTable(conf, "HFS_TABLE");
            InputStream is = new FileInputStream(INPUTFILE);
            FSPut fsPut = new FSPut(Bytes.toBytes("FILE_ID_1"));
            fsPut.addFile("I", is);
            fsTable.put(fsPut);
            LOG.info("Put file to HBase successfully.");
        }
        catch (IOException e)
        {
            throw new IOException("Put file failed!", e);
        }
        finally
        {
            if (fsTable != null)
            {
                try
                {
                    fsTable.close();
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Get files from HBase.
     */
    public void getFiles() throws IOException
    {
        FSTable fsTable = null;
        try
        {
            fsTable = new FSTable(conf, "HFS_TABLE");

            // get FSFile
            FSGet fsGet = new FSGet(Bytes.toBytes("FILE_ID_1"));
            fsGet.addFile("I");
            FSResult fsResult = fsTable.get(fsGet);

            FSFile fsFile = fsResult.getFile("I");
            if (fsFile == null)
            {
                throw new IOException("File isn't exits : " + "FILE_ID_1");
            }

            InputStream is = fsFile.createInputStream();

        }
        catch (IOException e)
        {
            throw new IOException("Get file failed!", e);
        }
        finally
        {
            if (fsTable != null)
            {
                try
                {
                    fsTable.close();
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }
    }

    private void cleanup() throws IOException
    {
        try (Admin admin = conn.getAdmin())
        {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        catch (IOException e)
        {
            throw new IOException("Delete table failed!", e);
        }
    }

}
