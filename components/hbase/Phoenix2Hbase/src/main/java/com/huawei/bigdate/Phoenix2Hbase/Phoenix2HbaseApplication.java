package com.huawei.bigdate.Phoenix2Hbase;

import com.huawei.bigdate.Phoenix2Hbase.service.TestPhoenix;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication(exclude={DataSourceAutoConfiguration.class, HibernateJpaAutoConfiguration.class})
@ComponentScan(basePackages = "com.huawei.bigdate.Phoenix2Hbase")
public class Phoenix2HbaseApplication {

	public static void main(String[] args) {
		SpringApplication.run(Phoenix2HbaseApplication.class, args);

		//初始化、登录
		TestPhoenix.initAndLogin();
	}

}
