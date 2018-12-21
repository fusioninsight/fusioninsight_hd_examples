package com.huawei.bigdata.spark.Body;

public class AdvertisingUserBody {
    int userID;
    String product;
    String userName;

    public void setUserID(int userID) {
        this.userID = userID;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public int getUserID() {
        return userID;
    }

    public String getProduct() {
        return product;
    }

    public String getUserName() {
        return userName;
    }
}
