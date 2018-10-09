package com.huawei.bigdata.hdfs.examples;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.huawei.hadoop.security.LoginUtil;

public class HdfsExample
{

    private static final String STORAGE_POLICY_HOT = "HOT";

    private FileSystem fSystem; /* HDFS file system */

    private Configuration conf;

    // private String DEST_PATH = "/user/hdfs-examples";
    private String DEST_PATH = "/flume/hdfs-examples";

    private String FILE_NAME = "test.txt";

    private static String PRNCIPAL_NAME = "test";

    private static String PATH_TO_KEYTAB = HdfsExample.class.getClassLoader().getResource("user.keytab").getPath();

    private static String PATH_TO_KRB5_CONF = HdfsExample.class.getClassLoader().getResource("krb5.conf").getPath();

    private static String PATH_TO_HDFS_SITE_XML = HdfsExample.class.getClassLoader().getResource("hdfs-site.xml")
            .getPath();

    private static String PATH_TO_CORE_SITE_XML = HdfsExample.class.getClassLoader().getResource("core-site.xml")
            .getPath();

    public HdfsExample(Configuration conf)
    {
        this.conf = conf;
        try
        {
            instanceBuild();
        }
        catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * HDFS operator instance
     * 
     * @throws IOException
     */
    public void examples()
    {
        // init HDFS FileSystem instance
        try
        {

            init(); // login from here

        }
        catch (IOException e)
        {
            System.err.println("Init hdfs filesystem failed.");
            e.printStackTrace();
            System.exit(1);
        }

        // operator file system
        try
        {
            // create directory
            mkdir();

            // write file
            write();

            // append file
            append();

            // read file
            read();

            // delete file
            delete();

            // delete directory
            rmdir();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * init get a FileSystem instance
     *
     * @throws IOException
     */
    private void init() throws IOException
    {
        confLoad();
        // authentication();
        instanceBuild();
    }

    /**
     * 
     * Add configuration file
     */
    private void confLoad() throws IOException
    {

        conf = new Configuration();
        // conf file
        // conf.addResource(new Path(PATH_TO_HDFS_SITE_XML));
        // conf.addResource(new Path(PATH_TO_CORE_SITE_XML));

        String userdir = System.getProperty("user.dir") + File.separator + "conf2" + File.separator;
        conf.addResource(new Path(userdir + "core-site.xml"));
        conf.addResource(new Path(userdir + "hdfs-site.xml"));

    }

    public Configuration getConf()
    {
        return conf;
    }

    /**
     * kerberos security authentication
     */
    private void authentication() throws IOException
    {
        // security mode
        if ("kerberos".equalsIgnoreCase(conf.get("hadoop.security.authentication")))
        {
            System.setProperty("java.security.krb5.conf", PATH_TO_KRB5_CONF);
            LoginUtil.login(PRNCIPAL_NAME, PATH_TO_KEYTAB, PATH_TO_KRB5_CONF, conf);
        }

    }

    /**
     * build HDFS instance
     */
    private void instanceBuild() throws IOException
    {

        // get filesystem
        try
        {
            fSystem = FileSystem.get(conf);
        }
        catch (IOException e)
        {
            throw new IOException("Get fileSystem failed.");
        }
    }

    /**
     * delete directory
     * 
     * @throws IOException
     */
    public void rmdir()
    {
        Path destPath = new Path(DEST_PATH);
        if (!deletePath(destPath))
        {
            System.err.println("failed to delete destPath " + DEST_PATH);
            return;
        }

        System.out.println("success to delete path " + DEST_PATH);

    }

    /**
     * create directory
     * 
     * @throws IOException
     */
    public void mkdir()
    {
        Path destPath = new Path(DEST_PATH);
        if (!createPath(destPath))
        {
            System.err.println("failed to create destPath " + DEST_PATH);
            return;
        }

        System.out.println("success to create path " + DEST_PATH);
    }

    /**
     * set storage policy to path
     * 
     * @param policyName Policy Name can be accepted:
     * <li>HOT
     * <li>WARN
     * <li>COLD
     * <li>LAZY_PERSIST
     * <li>ALL_SSD
     * <li>ONE_SSD
     * @throws IOException
     */
    // private void setStoragePolicy(String policyName) throws IOException {
    // DistributedFileSystem dfs = (DistributedFileSystem) fSystem;
    // Path destPath = new Path(DEST_PATH);
    // Boolean flag = false;
    // mkdir();
    // BlockStoragePolicySpi[] storage = dfs.getStoragePolicies();
    // for (BlockStoragePolicySpi bs : storage) {
    // if (bs.getName().equals(policyName)) {
    // flag = true;
    // }
    // System.out.println("StoragePolicy:" + bs.getName());
    // }
    // if (!flag && storage.length > 0) {
    // policyName = storage[0].getName();
    // }
    // dfs.setStoragePolicy(destPath, policyName);
    // System.out.println("success to set Storage Policy path " + DEST_PATH);
    // rmdir();
    // }

    /**
     * create file,write file
     * 
     * @throws IOException
     */
    public void write() throws IOException
    {
        final String content = "hi, I am bigdata. It is successful if you can see me.";
        InputStream in = (InputStream) new ByteArrayInputStream(content.getBytes());
        try
        {
            HdfsWriter writer = new HdfsWriter(fSystem, DEST_PATH + File.separator + FILE_NAME);
            writer.doWrite(in);
            System.out.println("success to write.");
        }
        catch (ParameterException e)
        {
            e.printStackTrace();
        }
        catch (IOException e2)
        {
            e2.printStackTrace();
            System.err.println("failed to write.");
        }
        finally
        {
            close(in);
        }
    }

    /**
     * append file content
     * 
     * @throws IOException
     */
    public void append() throws IOException
    {
        final String content = "I append this content.";
        InputStream in = (InputStream) new ByteArrayInputStream(content.getBytes());
        try
        {
            HdfsWriter writer = new HdfsWriter(fSystem, DEST_PATH + File.separator + FILE_NAME);
            writer.doAppend(in);
            System.out.println("success to append.");
        }
        catch (ParameterException e)
        {
            e.printStackTrace();
        }
        catch (IOException e2)
        {
            e2.printStackTrace();
            System.err.println("failed to append.");
        }
        finally
        {
            close(in);
        }
    }

    /**
     * read file
     * 
     * @throws IOException
     */
    public void read() throws IOException
    {
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

            System.out.println("result is : " + strBuffer.toString());
            System.out.println("success to read.");

        }
        catch (IOException e)
        {
            e.printStackTrace();
            System.err.println("failed to read.");
        }
        finally
        {
            close(reader);
            close(in);
        }
    }

    /**
     * delete file
     * 
     * @throws IOException
     */
    public void delete() throws IOException
    {
        Path beDeletedPath = new Path(DEST_PATH + File.separator + FILE_NAME);
        try
        {
            fSystem.deleteOnExit(beDeletedPath);
            System.out.println("succeess to delete the file " + DEST_PATH + File.separator + FILE_NAME);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            System.err.println("failed to delete the file " + DEST_PATH + File.separator + FILE_NAME);
        }
    }

    /**
     * close stream
     * 
     * @param stream
     */
    private void close(Closeable stream)
    {
        try
        {
            stream.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * create file path
     * 
     * @param filePath
     * @return
     */
    private boolean createPath(final Path filePath)
    {
        try
        {
            if (!fSystem.exists(filePath))
            {
                fSystem.mkdirs(filePath);
            }
            return true;
        }
        catch (IOException e)
        {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * delete file path
     * 
     * @param filePath
     * @return
     */
    private boolean deletePath(final Path filePath)
    {
        try
        {
            if (!fSystem.exists(filePath))
            {
                return true;
            }

            fSystem.delete(filePath, true);

        }
        catch (IOException e)
        {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    // public static void main(String[] args) {
    // HdfsExample hdfs_examples = new HdfsExample();
    // hdfs_examples.examples();
    // // set Storage Policy
    // System.out.println("begin to set Storage Policy");
    //
    //// try {
    //// hdfs_examples.setStoragePolicy(STORAGE_POLICY_HOT);
    //// } catch (IOException e) {
    //// e.printStackTrace();
    //// }
    //
    // System.out.println("set Storage Policy end");
    // }

}
