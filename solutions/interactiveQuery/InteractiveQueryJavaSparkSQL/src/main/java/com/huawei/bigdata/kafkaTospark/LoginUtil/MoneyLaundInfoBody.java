package com.huawei.bigdata.kafkaTospark.LoginUtil;

public class MoneyLaundInfoBody {
    String bankAcount;
    String identDate;

    public String getBankAcount() {
        return bankAcount;
    }

    public String getIdentDate() {
        return identDate;
    }

    public void setBankAcount(String bankAcount) {
        this.bankAcount = bankAcount;
    }

    public void setIdentDate(String identDate) {
        this.identDate = identDate;
    }
}
