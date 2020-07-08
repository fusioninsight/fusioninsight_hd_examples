package com.huawei.bigdata.body;
//创建数据库

import java.util.HashMap;

//:db 数据库名
//group 创建数据库时使用的用户组
//permission 创建数据库时使用的权限
//location 数据库的位置
//comment 数据库的备注，比如描述
// properties 数据库属性
public class ReqCreateDataBaBody {
    public String name ;
    public String group;
    public String permission ;
    public HashMap<String,String> properties;
    public String location ;
    public String comment;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getPermission() {
        return permission;
    }

    public void setPermission(String permission) {
        this.permission = permission;
    }

    public HashMap<String, String> getProperties() {
        return properties;
    }

    public void setProperties(HashMap<String, String> properties) {
        this.properties = properties;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public String toString() {
        return "ReqCreateDataBaBody{" +
                "properties=" + properties +
                ", location='" + location + '\'' +
                ", comment='" + comment + '\'' +
                '}';
    }
}
