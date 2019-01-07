package com.huawei.bigdata.kafkaTospark.LoginUtil;

public class TransactionInfoBody {
    String bankAcount;
    String transDate;
    String transMoney;
    String transType;
    String targetAcount;
    String remarks;

    public String getBankAcount() {
        return bankAcount;
    }

    public String getTransDate() {
        return transDate;
    }

    public String getTransMoney() {
        return transMoney;
    }

    public String getTransType() {
        return transType;
    }

    public String getTargetAcount() {
        return targetAcount;
    }

    public String getRemarks() {
        return remarks;
    }

    public void setBankAcount(String bankAcount) {
        this.bankAcount = bankAcount;
    }

    public void setTransDate(String transDate) {
        this.transDate = transDate;
    }

    public void setTransMoney(String transMoney) {
        this.transMoney = transMoney;
    }

    public void setTransType(String transType) {
        this.transType = transType;
    }

    public void setTargetAcount(String targetAcount) {
        this.targetAcount = targetAcount;
    }

    public void setRemarks(String remarks) {
        this.remarks = remarks;
    }
}
