package com.huawei.hadoop.webhdfs.client;

import org.apache.hadoop.security.authentication.client.AuthenticationException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class HDFSMain {
	private static KerberosWebHDFSConnection conn = null;
	
	public static void main(String[] args) throws IOException, AuthenticationException{
		init();
		getHomeDirectory();
	}
	
    public static void getHomeDirectory() throws IOException, AuthenticationException {

        String json = conn.getHomeDirectory();
        System.out.println(json);
    }
	
	public static void init(){
        String url = "https://host189-132-134-121:25003";
        String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
        String userName = "tester1";
        String userKeytabFile = userdir + "user.keytab";
        String krb5File = userdir + "krb5.conf";
        conn = new KerberosWebHDFSConnection(url, krb5File, userKeytabFile, userName);
	}

}
