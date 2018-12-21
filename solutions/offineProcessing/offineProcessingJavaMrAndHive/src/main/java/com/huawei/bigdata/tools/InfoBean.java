package com.huawei.bigdata.tools;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class InfoBean  implements Writable {
    private int userID;
    private String produce;
    private String produceType;
    private int price;
    private String shoppingDate;

    private String userName;
    private String registrationDate;
    private int flag;
    public InfoBean(){}
    public void  set (int userID, String produce, String produceType, int price, String shoppingDate, String userName,
                     String registrationDate,int flag) {
        this.userID = userID;
        this.produce = produce;
        this.produceType = produceType;
        this.price = price;
        this.shoppingDate = shoppingDate;
        this.userName = userName;
        this.registrationDate = registrationDate;
        this.flag=flag;
    }
    public int getUserID() {
        return userID;
    }
    public void setUserID(int userID) {
        this.userID = userID;
    }
    public String getProduce() {
        return produce;
    }
    public void setProduce(String produce) {
        this.produce = produce;
    }
    public String getProduceType() {
        return produceType;
    }
    public void setProduceType(String produceType) {
        this.produceType = produceType;
    }
    public int getPrice() {
        return price;
    }
    public void setPrice(int price) {
        this.price = price;
    }
    public String getShoppingDate() {
        return shoppingDate;
    }
    public void setShoppingDate(String shoppingDate) {
        this.shoppingDate = shoppingDate;
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
    public int getFlag() {
        return flag;
    }
    public void setFlag(int flag) {
        this.flag = flag;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(userID);
        dataOutput.writeUTF(produce);
        dataOutput.writeUTF(produceType);
        dataOutput.writeInt(price);
        dataOutput.writeUTF(shoppingDate);
        dataOutput.writeUTF(userName);
        dataOutput.writeUTF(registrationDate);
        dataOutput.writeInt(flag);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.userID=dataInput.readInt();
        this.produce = dataInput.readUTF();
        this.produceType = dataInput.readUTF();
        this.price = dataInput.readInt();
        this.shoppingDate = dataInput.readUTF();
        this.userName = dataInput.readUTF();
        this.registrationDate = dataInput.readUTF();
        this.flag=dataInput.readInt();
    }
    @Override
    public String toString() {
        return userID + "," + produce + "," + produceType + ","
                + price + "," + shoppingDate + "," + userName + ","
                + registrationDate + "," + flag;
    }
}
