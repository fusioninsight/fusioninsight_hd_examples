package com.huawei.fusioninsight.elasticsearch.example.util;

import org.apache.commons.codec.binary.Base64;

public class Base64Utils {

    public static void main(String[] args) {
        //对字符串"userName:passwd"进行base64加密
        System.out.println(encodeBase64("userName:passwd"));
        //对已加密的字符串"dXNlck5hbWU6cGFzc3dk"进行base64解密
        System.out.println(decodeBase64("dXNlck5hbWU6cGFzc3dk"));
    }

    // base64加密
    private static String encodeBase64(String needEncodeString) {
        return Base64.encodeBase64String(needEncodeString.getBytes());
    }

    //base64解密
    private static String decodeBase64(String needDecodeBase64Str) {
        byte[] result = Base64.decodeBase64(needDecodeBase64Str.getBytes());
        return new String(result);
    }
}
