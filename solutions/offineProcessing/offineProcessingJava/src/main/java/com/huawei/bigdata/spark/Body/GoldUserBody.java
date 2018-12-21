package com.huawei.bigdata.spark.Body;

public class GoldUserBody {
    int userId ;
    String userName;
    int Amount;

    public int getUserId() {
        return userId;
    }

    public String getUserName() {
        return userName;
    }

    public int getAmount() {
        return Amount;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public void setAmount(int amount) {
        Amount = amount;
    }
}
