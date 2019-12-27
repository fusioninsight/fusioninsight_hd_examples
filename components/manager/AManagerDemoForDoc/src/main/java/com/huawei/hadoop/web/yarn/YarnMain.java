package com.huawei.hadoop.web.yarn;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.security.authentication.client.AuthenticationException;

public class YarnMain {
	private static KerberosWebYarnConnection conn = null;
	
	public static void main(String[] args) throws IOException, AuthenticationException{
		init();
		String reslut = conn.getApps();
		System.out.println(reslut.length());
		System.out.println("apps:" + reslut);
	}
	
	/**
	 * �����֤����ȡ����ʵ��
	 */
	public static void init(){
      //��RM��IP�Ͷ˿� 
	  //String url = "https://8-5-214-7:26001";
      String url = "https://189.211.68.223:26001";
	 //String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
	  String userdir = "D:\\04step2\\00CODE\\HD\\components\\manager\\AManagerDemoForDoc\\src\\main\\resources\\conf\\";
      String userName = "fanC80_jj";
      String userKeytabFile = userdir + "user.keytab";
      String krb5File = userdir + "krb5.conf";
      conn = new KerberosWebYarnConnection(url, krb5File, userKeytabFile, userName);
	}
}
