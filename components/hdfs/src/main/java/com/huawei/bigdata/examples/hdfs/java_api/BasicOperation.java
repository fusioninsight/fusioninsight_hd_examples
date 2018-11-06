package com.huawei.bigdata.examples.hdfs.java_api;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Scanner;

import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.*;
import org.apache.log4j.PropertyConfigurator;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

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
    private final String FILE_NAME;//本地文件

    public BasicOperation(String path, String fileName) throws IOException
    {

        this.DEST_PATH = path;
        this.FILE_NAME = fileName;
        instanceBuild();
    }

    private static void confLoad() throws IOException
    {    //创建配置文件对象
        conf = new Configuration();

        //加载HDFS服务端配置，包含客户端与服务端对接配置
        conf.addResource(new Path(BasicOperation.class.getClassLoader().getResource("conf/core-site.xml").getPath()));
        conf.addResource(new Path(BasicOperation.class.getClassLoader().getResource("conf/hdfs-site.xml").getPath()));//把它们合并成一个配置
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
        fSystem = FileSystem.get(conf);//返回配置的FileSystem实现。
    }

    private static void authentication() throws IOException
    {
        //如果配置文件中指定为kerberos认证，需要登录认证。只在系统启动时执行一次
        if (!"kerberos".equalsIgnoreCase(conf.get("hadoop.security.authentication")))
        {
            return;
        }

        //认证相关，安全模式需要，普通模式可以删除
        String PRNCIPAL_NAME = "lyysxg";//需要修改为实际在manager添加的用户
        String KRB5_CONF = BasicOperation.class.getClassLoader().getResource("conf/krb5.conf").getPath();
        String KEY_TAB = BasicOperation.class.getClassLoader().getResource("conf/user.keytab").getPath();

        System.setProperty("java.security.krb5.conf", KRB5_CONF); //指定kerberos配置文件到JVM
        LoginUtil.login(PRNCIPAL_NAME, KEY_TAB, KRB5_CONF, conf);
    }

    /**
     * //创建目录对象
     * @throws IOException
     */
    private void mkdir() throws IOException
    {
        LOG.info("=========== mkdir() begin ===========");

        Path destPath = new Path(DEST_PATH);//创建目录的路径
        if (!createPath(destPath))//调方法
        {
            LOG.error("failed to create destPath " + DEST_PATH);
            return;
        }

        LOG.info("success to create path " + DEST_PATH);
        LOG.info("=========== mkdir() end ===========");
    }

    /**
     * 删除目录
     * @throws IOException
     */
    private void rmdir() throws IOException
    {
        Path destPath = new Path(DEST_PATH);//删除文件的路径
        if (!deletePath(destPath))//调删除的方法
        {
            LOG.error("failed to delete destPath " + DEST_PATH);
            return;
        }

        LOG.info("success to delete path " + DEST_PATH);

    }

    /**
     * create file,write file写文件
     *
     * @throws IOException
     * @throws com.huawei.bigdata.hdfs.examples.ParameterException
     */
    private void write() throws IOException
    {
        LOG.info("=========== write() begin ===========");

        final String content = "hi, I am bigdata. It is successful if you can see me.";
        FSDataOutputStream out = null;
        try
        {
            //创建文件写入流
            out = fSystem.create(new Path(DEST_PATH + File.separator + FILE_NAME));

            //写入数据
            out.write(content.getBytes());

            //将客户端用户缓冲区中的数据一直刷新到磁盘设备（但磁盘可能将其放在缓存中）
            out.hsync();

            LOG.info("success to write.");
        }
        finally
        {
            // make sure the stream is closed finally.
            if (out != null)
            {
                //关闭基础输出流
                out.close();
            }
        }

        LOG.info("=========== write() end ===========");
    }

    /**
     * append file content
     *
     * @throws IOException
     */
    //追加写入
    private void append() throws IOException
    {
        LOG.info("=========== append() begin ===========");

        final String content = "I append this content.";
        FSDataOutputStream out = null;
        try
        {
            out = fSystem.append(new Path(DEST_PATH + File.separator + FILE_NAME));//追加一个输入流
            out.write(content.getBytes());
            out.hsync();
            LOG.info("success to append.");
        }
        finally
        {
            //写入完成后关闭数据流.
            out.close();

        }

        LOG.info("=========== append() end ===========");
    }

    /**
     * read file
     *
     * @throws IOException
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
        {    //创建文件读取流
            in = fSystem.open(path);
            //缓冲文件读取流
            reader = new BufferedReader(new InputStreamReader(in));
            String sTempOneLine;

            // 读取文件数据
            while ((sTempOneLine = reader.readLine()) != null)
            {
                strBuffer.append(sTempOneLine);
            }

            LOG.info("result is : " + strBuffer.toString());
            LOG.info("success to read.");

        }
        finally
        {    //确保关闭已开启的数据流
            // make sure the streams are closed finally.
            close(reader);
            close(in);
        }

        LOG.info("=========== read() end ===========");
    }

    /**
     * delete file
     *
     * @throws IOException
     */
    private void delete() throws IOException
    {
        LOG.info("=========== delete() begin ===========");

        Path beDeletedPath = new Path(DEST_PATH + File.separator + FILE_NAME);

        //清理环境，删除目录和文件
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
     * @throws IOException
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
     * @throws IOException
     */
    private boolean createPath(final Path filePath) throws IOException
    {
        if (!fSystem.exists(filePath))//查看是否有这个文件
        {
            fSystem.mkdirs(filePath);//没有，就创建，有就返回
        }
        return true;
    }
    private void UploadLocalFile(){
        try {
            Path src = new Path(FILE_NAME);//本地文件路径
            Path dst = new Path(DEST_PATH);//上传到HDFS的路径
            fSystem.copyFromLocalFile(src, dst);//上传文件的命令
            LOG.info("上传成功！！！");
            fSystem.close();//关闭
        }catch (Exception e){
            LOG.error(e);
        }
    }
    /**
     * delete file path
     *
     * @param filePath
     * @return
     * @throws IOException
     */
    private boolean deletePath(final Path filePath) throws IOException
    {
        if (!fSystem.exists(filePath))
        {
            return false;
        }
        //删除文件，第二个参数表示遍历删除该目录下的子目录或文件
        return fSystem.delete(filePath, true);
    } private  void rename() throws  IOException
{
    try
    {
        Path destPath = new Path(DEST_PATH);//路径
        Scanner scan = new Scanner(System.in);//控制台获得输入的文本
        FileStatus[] files =  fSystem.listStatus(destPath);//路径下所有的目录和文件

        System.out.println("当前目录下的文件信息如下：");
        for (FileStatus fs :files){//遍历

            System.out.println("\t" + fs.getPath().getName());
        }
        System.out.println("输入要修改的文件名字：");
        String oldName = scan.nextLine();//接收控制台的文字
        Path oldPath = new Path(destPath+"/"+oldName);//确定某一个文本
        System.out.println("输入要修改后的文件名字：");
        Path newName = new Path(destPath+"/"+scan.next());
        boolean isResult = fSystem.rename(oldPath,newName);
        LOG.info("名字修改" + (isResult ? "成功！":"失败！"));
    }catch (Exception e)
    {
        LOG.error("操作失败"+e);
    }
}
    private  void select()throws  IOException
    {
        Path path = new Path(DEST_PATH+File.separator+FILE_NAME);//文件的路径
        if(!fSystem.exists(path))//判断该路径下是否有这个文件
        {
            LOG.info("file does not exist");
        }else{
            LOG.info("fileexist"+path);
        }
    }
    private  void getAllFile()throws  IOException
    {
        Path pate = new Path(DEST_PATH);
        FileStatus[] files = fSystem.listStatus(pate);//FileStatus方法可以获得文件或者目录的属性信息
        if(files.length==0)//files==0说明，这个路径下没有目录和文件
        {
            LOG.warn("file does not exist" + DEST_PATH);
        }else {
            System.out.println("当前目录下的文件信息如下：");
            for (FileStatus fs : files)//遍历输出这个路径下的所有目录和文件
            {
                LOG.info(fs.getPath().getName());

            }
        }
    }
    //查看指定文件最后修改时间
    private void changeTime() throws  IOException
    {

        Path destPath = new Path(DEST_PATH+File.separator+FILE_NAME);//路径加文件

        if(!fSystem.exists(destPath))//判断是否存在
        {

            LOG.warn("文件不存在");
            return;
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:MM:ss");//设置日期输出格式
        FileStatus fs = fSystem.getFileStatus(destPath);//获得这个文件的属性
        LOG.info("最后修改时间"+sdf.format(fs.getModificationTime()));//getModificationTime获得文件的修改时间
    }

    //解释方法作用
    private  void getBlockInfro()throws IOException
    {
        Path path = new Path(DEST_PATH+File.separator+FILE_NAME);
        FileStatus file = fSystem.getFileStatus(path);

        //BlockLocation对象是获取
        //该方法下三个参数，第一个是文件的名字，第二和第三个是该文件指定部分在集群中的位置(比如，从起始位置0开始，获取file.getLen()长度的数据))
        BlockLocation[] blkoc = fSystem.getFileBlockLocations(file,0,file.getLen());//查找文件所在数据块
        for (int i = 0; i <blkoc.length ; i++) {
            LOG.info("blkoc所在位置"+blkoc[i].getHosts().toString());//getHosts()获得托管块的缓存副本的主机列表
        }
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
        //上传本地文件
        this.UploadLocalFile();
        //重命名
       this.rename();
        //查找文件是否存在
        this.select();
        //读取某个目录下的所有文件
        this.getAllFile();
        //查找某个文件在集群的位置
        this.getBlockInfro();
       // 查看文件的最后修改时间
        this.changeTime();
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
        BasicOperation basicOperation = new BasicOperation("/user11/hdfs-examples", "C:\\Users\\zwx613270\\Desktop\\linux命令.txt");
        basicOperation.test();

        LOG.info("***************** BasicOperation End *****************");
    }
}


