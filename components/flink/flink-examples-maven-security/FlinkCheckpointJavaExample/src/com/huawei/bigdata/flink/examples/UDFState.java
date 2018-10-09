package com.huawei.bigdata.flink.examples;

import java.io.Serializable;

public class UDFState implements Serializable
{
    private long count;

    public UDFState()
    {
        count = 0L;
    }

    public void setState(long count)
    {
        this.count = count;
    }

    public long getState()
    {
        return this.count;
    }

}
