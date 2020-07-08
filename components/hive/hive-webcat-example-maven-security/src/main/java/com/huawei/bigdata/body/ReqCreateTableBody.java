package com.huawei.bigdata.body;

import javax.lang.model.element.NestingKind;
import java.util.HashMap;
import java.util.List;

/*        :db	数据库名
        :table	新建表名
        group	创建表时使用的用户组
        permissions	创建表时使用的权限
        external	指定位置，hive不使用表的默认位置。
        ifNotExists	设置为true，当表存在时不会报错。
        comment	备注
        columns	列描述，包括列名，类型和可选备注。
        partitionedBy	分区列描述，用于划分表格。参数columns列出了列名，类型和可选备注。
        clusteredBy	分桶列描述，参数包括columnNames、sortedBy、和numberOfBuckets。参数columnNames包括columnName和排列顺序（ASC为升序，DESC为降序）。
        format	存储格式，参数包括rowFormat，storedAs，和storedBy。
        location	HDFS路径
        tableProperties	表属性和属性值（name-value对）*/
public class ReqCreateTableBody {
    String group;
    String permissions;
    String external;
    String ifNotExists;
    String comment;
    List<HashMap<String,String>> columns;
    List<HashMap<String,String>> partitionedBy;
    HashMap<String,Object> clusteredBy;
    HashMap<String,Object> format;
    String location;
    String ReqCreateTableBody;
    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getPermissions() {
        return permissions;
    }

    public void setPermissions(String permissions) {
        this.permissions = permissions;
    }

    public String getExternal() {
        return external;
    }

    public void setExternal(String external) {
        this.external = external;
    }

    public String getIfNotExists() {
        return ifNotExists;
    }

    public void setIfNotExists(String ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public List<HashMap<String, String>> getColumns() {
        return columns;
    }

    public void setColumns(List<HashMap<String, String>> columns) {
        this.columns = columns;
    }

    public List<HashMap<String, String>> getPartitionedBy() {
        return partitionedBy;
    }

    public void setPartitionedBy(List<HashMap<String, String>> partitionedBy) {
        this.partitionedBy = partitionedBy;
    }

    public HashMap<String, Object> getClusteredBy() {
        return clusteredBy;
    }

    public void setClusteredBy(HashMap<String, Object> clusteredBy) {
        this.clusteredBy = clusteredBy;
    }

    public HashMap<String, Object> getFormat() {
        return format;
    }

    public void setFormat(HashMap<String, Object> format) {
        this.format = format;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getReqCreateTableBody() {
        return ReqCreateTableBody;
    }

    public void setReqCreateTableBody(String reqCreateTableBody) {
        ReqCreateTableBody = reqCreateTableBody;
    }
}
