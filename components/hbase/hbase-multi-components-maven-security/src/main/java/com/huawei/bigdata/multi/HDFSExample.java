package com.huawei.bigdata.multi;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.token.Token;

public class HDFSExample {
	private FileSystem fSystem; /* HDFS file system */
	private static Configuration conf;
	private String DEST_PATH = "/user/hdfs-examples";
	private String FILE_NAME = "zhuo.txt";
	
	public HDFSExample(Configuration conf) throws IOException {
		    this.conf = conf;
		    this.fSystem = FileSystem.get(this.conf);
	}
	
	/**
	 * HDFS operator instance
	 * 
	 * @throws Exception
	 *
	 */
	public void test() throws Exception {
		mkdir();
		write();
		// append file
		append();

		// read file
		read();
	}
		

	/**
	 * delete directory
	 *
	 * @throws java.io.IOException
	 */
	private void rmdir() throws IOException {
		Path destPath = new Path(DEST_PATH);
		if (!deletePath(destPath)) {
			System.err.println("failed to delete destPath " + DEST_PATH);
			return;
		}

		System.out.println("success to delete path " + DEST_PATH);

	}

	/**
	 * create directory
	 *
	 * @throws java.io.IOException
	 */
	public void mkdir() throws IOException {
		Path destPath = new Path(DEST_PATH);
		if (!createPath(destPath)) {
			System.err.println("failed to create destPath " + DEST_PATH);
			return;
		}

		System.out.println("success to create path " + DEST_PATH);
	}

	/**
	 * create file,write file
	 *
	 * @throws java.io.IOException
	 * @throws com.huawei.bigdata.hdfs.examples.ParameterException
	 */
	private void write() throws IOException, ParameterException {
		final String content = "hi, I am bigdata. It is successful if you can see me.";
		InputStream in = (InputStream) new ByteArrayInputStream(content.getBytes());
		try {
			HdfsWriter writer = new HdfsWriter(fSystem, DEST_PATH + File.separator + FILE_NAME);
			writer.doWrite(in);
			System.out.println("success to write.");
		} finally {
			// make sure the stream is closed finally.
			close(in);
		}
	}

	/**
	 * append file content
	 *
	 * @throws java.io.IOException
	 */
	private void append() throws Exception {
		final String content = "I append this content.";
		InputStream in = (InputStream) new ByteArrayInputStream(content.getBytes());
		try {
			HdfsWriter writer = new HdfsWriter(fSystem, DEST_PATH + File.separator + FILE_NAME);
			writer.doAppend(in);
			System.out.println("success to append.");
		} finally {
			// make sure the stream is closed finally.
			close(in);
		}
	}

	/**
	 * read file
	 *
	 * @throws java.io.IOException
	 */
	private void read() throws IOException {
		String strPath = DEST_PATH + File.separator + FILE_NAME;
		Path path = new Path(strPath);
		FSDataInputStream in = null;
		BufferedReader reader = null;
		StringBuffer strBuffer = new StringBuffer();

		try {
			in = fSystem.open(path);
			reader = new BufferedReader(new InputStreamReader(in));
			String sTempOneLine;

			// write file
			while ((sTempOneLine = reader.readLine()) != null) {
				strBuffer.append(sTempOneLine);
			}

			System.out.println("result is : " + strBuffer.toString());
			System.out.println("success to read.");

		} finally {
			// make sure the streams are closed finally.
			close(reader);
			close(in);
		}
	}

	/**
	 * delete file
	 *
	 * @throws java.io.IOException
	 */
	private void delete() throws IOException {
		Path beDeletedPath = new Path(DEST_PATH + File.separator + FILE_NAME);
		if (fSystem.delete(beDeletedPath, true)) {
			System.out.println("success to delete the file " + DEST_PATH + File.separator + FILE_NAME);
		} else {
			System.out.println("failed to delete the file " + DEST_PATH + File.separator + FILE_NAME);
		}
	}

	/**
	 * close stream
	 *
	 * @param stream
	 * @throws java.io.IOException
	 */
	private void close(Closeable stream) throws IOException {
		stream.close();
	}
	
	public void closeConnection(){
		try {
			this.fSystem.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * create file path
	 *
	 * @param filePath
	 * @return
	 * @throws java.io.IOException
	 */
	private boolean createPath(final Path filePath) throws IOException {
		if (!fSystem.exists(filePath)) {
			fSystem.mkdirs(filePath);
		}
		Token<?> result = fSystem.getDelegationToken("tester1");
		System.out.println("result identifier:" + (new String(result.getIdentifier())));
		System.out.println("result getKind:" + result.getKind());
		System.out.println("result getPassword:" + (new String(result.getPassword())));
		System.out.println("result service:" + result.getService());
		//System.out.println("result service:" + result.re);
		
		Token<?> result2 = fSystem.getDelegationToken("HDFS_DELEGATION_TOKEN");
		System.out.println("result2 identifier:" + (new String(result2.getIdentifier())));
		System.out.println("result2 getKind:" + result2.getKind());
		System.out.println("result2 getPassword:" + (new String(result2.getPassword())));
		System.out.println("result2 service:" + result2.getService());
		
		return true;
	}

	/**
	 * delete file path
	 *
	 * @param filePath
	 * @return
	 * @throws java.io.IOException
	 */
	private boolean deletePath(final Path filePath) throws IOException {
		if (!fSystem.exists(filePath)) {
			return false;
		}
		// fSystem.delete(filePath, true);
		return fSystem.delete(filePath, true);
	}

//	public static void main(String[] args) throws Exception {
//		HdfsExample hdfs_examples = new HdfsExample();
//		hdfs_examples.test();
//	}

}
