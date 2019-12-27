package com.huawei.bigdata.esandhbase.example;

import java.util.List;

public class ESAndHbaseSearch {
    /**
     * ES HBASE联合查询：
     * 根据人名 从ES中查出ID ，再根据ID从HBASE查出所有信息
    */
    public static void main(String[] args) {
        // 1.根据人名从ES查出ID
        List<String> ids = ESSearch.getID("怀白晴");
        // 2.根据id 从HBASE 查出其他数据
        for (String id : ids) {
            HbaseSearch.search(id);
        }
    }
}
