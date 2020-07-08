package com.huawei.bigdata.body;
//删除数据库参数
public class ReqDelTableBody {
    public String tabelName;
    public String dataBaseName;
    public String ifExists;
    public String option;

    public String getDataBaseName() {
        return dataBaseName;
    }

    public void setDataBaseName(String dataBaseName) {
        this.dataBaseName = dataBaseName;
    }

    public String getTabelName() {
        return tabelName;
    }

    public void setTabelName(String tabelName) {
        this.tabelName = tabelName;
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
}
