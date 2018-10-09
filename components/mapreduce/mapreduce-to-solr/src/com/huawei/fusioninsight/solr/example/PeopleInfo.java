package com.huawei.fusioninsight.solr.example;

public class PeopleInfo {
	
	String name;
	String sex;
	int age;
	
	public PeopleInfo(String name, String sex, int age){
		this.name = name;
		this.sex = sex;
		this.age = age;
	}
	
	public String getName(){
		return this.name;
	}
	
	public String getSex(){
		return this.sex;
	}
	
	public int getAge(){
		return this.age;
	}

}
