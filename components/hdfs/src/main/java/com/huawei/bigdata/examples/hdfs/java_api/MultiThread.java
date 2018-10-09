package com.huawei.bigdata.examples.hdfs.java_api;

import java.io.IOException;
import org.apache.log4j.PropertyConfigurator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
   * HDFS多线程样例
   */

class MultiThread extends Thread
{
    {
        //日志配置文件
        PropertyConfigurator.configure(MultiThread.class.getClassLoader().getResource("conf/log4j.properties").getPath());
    }
    private final static Log LOG = LogFactory.getLog(MultiThread.class.getName());

    /**
     *
     * @param threadName
     */
    public MultiThread(String threadName)
    {
        super(threadName);
    }

    @Override
    public void run()
    {
        BasicOperation example;
        try
        {
            example = new BasicOperation("/user/hdfs-examples/" + getName(), "test.txt");
            example.test();
        }
        catch (IOException e)
        {
            LOG.error(e);
        }
    }

    public static void main(String[] args) throws Exception
    {
        final int THREAD_COUNT = 2;
        for (int threadNum = 0; threadNum < THREAD_COUNT; threadNum++)
        {
            MultiThread multiThread = new MultiThread("hdfs_example_" + threadNum);
            multiThread.start();
        }
    }
}
