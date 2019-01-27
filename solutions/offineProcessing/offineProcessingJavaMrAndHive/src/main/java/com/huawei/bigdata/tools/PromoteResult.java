package com.huawei.bigdata.tools;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PromoteResult {

    private String userName;
    private String produce;
    public PromoteResult(){}
    public void set( String userName,String produce)
    {
        this.userName = userName;
        this.produce = produce;

    }
    @Override
    public String toString() {
        return "userName=" + userName + ", produce=" + produce;
    }
    public String getUserName() {
        return userName;
    }
    public void setUserName(String userName) {
        this.userName = userName;
    }
    public String getProduce() {
        return produce;
    }
    public void setProduce(String produce) {
        this.produce = produce;
    }



    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeUTF(userName);
        dataOutput.writeUTF(produce);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.userName=dataInput.readUTF();
        this.produce=dataInput.readUTF();

    }
}
