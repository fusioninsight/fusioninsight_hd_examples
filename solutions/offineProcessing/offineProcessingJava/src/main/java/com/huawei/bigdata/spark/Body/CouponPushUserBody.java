package com.huawei.bigdata.spark.Body;

public class CouponPushUserBody {
    int userID;
    String userName;
    int amount;
    double money;

    public int getUserID() {
        return userID;
    }

    public void setUserID(int userID) {
        this.userID = userID;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public void setMoney(double money) {
        this.money = money;
    }

    public String getUserName() {
        return userName;
    }

    public int getAmount() {
        return amount;
    }

    public double getMoney() {
        return money;
    }
}
