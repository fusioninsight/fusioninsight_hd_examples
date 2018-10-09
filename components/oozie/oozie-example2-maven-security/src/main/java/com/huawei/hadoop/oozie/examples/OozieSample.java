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
package com.huawei.hadoop.oozie.examples;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;

public class OozieSample
{
    private static String JOB_PROPERTIES_FILE = OozieSample.class.getClassLoader().getResource("conf/job.properties")
            .getPath();

    private boolean isSecury;

    private OozieClient wc = null;

    public OozieSample(boolean isSecurityCluster) throws IOException
    {
        this.isSecury = isSecurityCluster;
        wc = new OozieClient(Constant.OOZIE_URL_DEFALUT);
    }

    public void test() throws Exception
    {
        try
        {
            System.out.println("cluset status is " + isSecury);
            if (isSecury)
            {
                UserGroupInformation.getLoginUser().doAs(new PrivilegedExceptionAction<Void>()
                {
                    public synchronized Void run() throws Exception
                    {
                        runJob();
                        return null;
                    }
                });
            }
            else
            {
                runJob();
            }

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

    }

    private void runJob() throws OozieClientException, InterruptedException
    {
        Properties conf = getJobProperties(JOB_PROPERTIES_FILE);

        // submit and start the workflow job
        String jobId = wc.run(conf);

        System.out.println("Workflow job submitted: " + jobId);

        // wait until the workflow job finishes printing the status every 10 seconds
        while (wc.getJobInfo(jobId).getStatus() == WorkflowJob.Status.RUNNING)
        {
            System.out.println("Workflow job running ..." + jobId);
            Thread.sleep(10 * 1000);
        }

        // print the final status of the workflow job
        System.out.println("Workflow job completed ..." + jobId);
        System.out.println(wc.getJobInfo(jobId));
    }

    public Properties getJobProperties(String filePath)
    {
        File configFile = new File(filePath);
        if (!configFile.exists())
        {
            System.out.println(filePath + " is not exist.");
        }

        InputStream inputStream = null;

        // create a workflow job configuration
        Properties p = wc.createConfiguration();
        try
        {
            inputStream = new FileInputStream(filePath);
            p.load(inputStream);

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            if (inputStream != null)
            {
                try
                {
                    inputStream.close();
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }

        return p;
    }

    private void killJob()
    {
        try
        {
            // wc.setDebugMode(1);
            Properties conf = getJobProperties(JOB_PROPERTIES_FILE);

            // submit and start the workflow job
            String jobId = wc.run(conf);
            System.out.println("create a new job : " + jobId);

            if (wc.getJobInfo(jobId).getStatus() == WorkflowJob.Status.SUCCEEDED)
            {
                System.out.println("you should not kill a done job : " + jobId);
                return;
            }

            wc.kill(jobId);

            WorkflowJob.Status currentStatus = wc.getJobInfo(jobId).getStatus();
            if (currentStatus == WorkflowJob.Status.KILLED)
            {
                System.out.println("it's success to kill the job : " + jobId);
            }
            else
            {
                System.out.println("failed to kill the job : " + jobId + ", the current status is " + currentStatus);
            }

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
