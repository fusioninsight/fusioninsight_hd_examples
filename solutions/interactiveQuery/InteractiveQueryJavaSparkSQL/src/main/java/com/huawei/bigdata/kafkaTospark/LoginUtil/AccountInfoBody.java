package com.huawei.bigdata.kafkaTospark.LoginUtil;

public class AccountInfoBody {
      String bankAccount;
      String AccountName;
      String IDCard;

    public String getBankAccount() {
        return bankAccount;
    }

    public String getAccountName() {
        return AccountName;
    }

    public String getIDCard() {
        return IDCard;
    }

    public void setBankAccount(String bankAccount) {
        this.bankAccount = bankAccount;
    }

    public void setAccountName(String accountName) {
        AccountName = accountName;
    }

    public void setIDCard(String IDCard) {
        this.IDCard = IDCard;
    }
}
