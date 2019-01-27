package com.huawei.bigdata.tools;


import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PromoteUsersInfo implements Writable
{
    private int userId;
    private String userName;
    private String registrationDate;;
    private String produce;
    private int time;
    private String viewDate;
    private int flag;
public PromoteUsersInfo(){}
    public void set(int userId, String userName, String registrationDate, String produce, int time,
                    String viewDate,int flag) {

        this.userId = userId;
        this.userName = userName;
        this.registrationDate = registrationDate;
        this.produce = produce;
        this.time = time;
        this.viewDate = viewDate;
        this.flag=flag;
    }
    @Override
    public String toString() {
        return "userId=" + userId + ", userName=" + userName + ", registrationDate=" + registrationDate
                + ", produce=" + produce + ", time=" + time + ", viewDate=" + viewDate +",flag="+flag;
    }
    public int getUserId() {
        return userId;
    }
    public void setUserId(int userId) {
        this.userId = userId;
    }
    public String getUserName() {
        return userName;
    }
    public void setUserName(String userName) {
        this.userName = userName;
    }
    public String getRegistrationDate() {
        return registrationDate;
    }
    public void setRegistrationDate(String registrationDate) {
        this.registrationDate = registrationDate;
    }
    public String getProduce() {
        return produce;
    }
    public void setProduce(String produce) {
        this.produce = produce;
    }
    public int getTime() {
        return time;
    }
    public void setTime(int time) {
        this.time = time;
    }
    public String getViewDate() {
        return viewDate;
    }
    public void setViewDate(String viewDate) {
        this.viewDate = viewDate;
    }
    public int getFlag() {
        return flag;
    }
    public void setFlag(int flag) {
        this.flag = flag;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(userId);
        dataOutput.writeUTF(userName);
        dataOutput.writeUTF(registrationDate);
        dataOutput.writeUTF(produce);
        dataOutput.writeInt(time);
        dataOutput.writeUTF(viewDate);
        dataOutput.writeInt(flag);

    }

    public void readFields(DataInput dataInput) throws IOException {
        this.userId=dataInput.readInt();
        this.userName=dataInput.readUTF();
        this.registrationDate=dataInput.readUTF();
        this.produce=dataInput.readUTF();
        this.time=dataInput.readInt();
        this.viewDate=dataInput.readUTF();
        this.flag=dataInput.readInt();
    }
}
