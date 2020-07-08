package com.huawei.bigdata.body;
//删除数据库参数
public class DeleteDataBaseBody {
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getIfExists() {
        return ifExists;
    }

    public void setIfExists(String ifExists) {
        this.ifExists = ifExists;
    }

    public String getOption() {
        return option;
    }

    public void setOption(String option) {
        this.option = option;
    }

    public String name;
    public String ifExists;
    public String option;
}
