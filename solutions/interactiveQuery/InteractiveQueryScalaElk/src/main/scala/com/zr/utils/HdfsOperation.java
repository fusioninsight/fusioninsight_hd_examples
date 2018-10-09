package main.scala.com.zr.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsOperation {
    public final static Configuration config = new Configuration();
    public static FileSystem hdfs;

    //
    public HdfsOperation() throws Exception {
        hdfs = FileSystem.get(config);
    }

    //创建hdfs文件
    public static void createFile(String path) throws Exception {
        Path path1 = new Path(path);
        hdfs.create(path1);
    }

    //追加内容
    public static void appendContext(String path, String txt) throws Exception {
        Path path2 = new Path(path);
        FSDataOutputStream os = hdfs.append(path2);
        os.write(txt.getBytes());
    }

    //查询判断文件是否存在
    public static boolean queryFile(String path) throws Exception {
        Path path3 = new Path(path);
        boolean is = hdfs.exists(path3);
        boolean isFile = false;
        if (is) {
            isFile = hdfs.isFile(path3);
        }
        return isFile;
    }

}
