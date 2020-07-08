package com.huawei.bigdata.information;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class RestHelper {
    public static String toJsonString(Object o) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        return objectMapper.writeValueAsString(o);
    }

    public static void checkHttpRsp(CloseableHttpResponse response) throws Exception {
        if (null == response) {

            throw new Exception("Http response error: response is null.");
        }
//        if(!response.getStatusLine().equals("OK")){
//            HttpEntity entity = response.getEntity();
//            System.out.println("----------------Status Start------------------------");
//            System.out.println(response.getStatusLine());
//            System.out.println("---------------EntityUtils Start-------------------------");
//            if (entity != null) {
//                System.out.println("---------------entity is not empty-------------------------");
//                String entiryString = EntityUtils.toString(entity);
//                System.out.println(entiryString);
//            }
//            System.out.println("----------EntityUtilsconsume Start------------------------------");
//            // This ensures the connection gets released back to the manager
//            EntityUtils.consume(entity);
//        }
        if (HttpStatus.SC_OK != response.getStatusLine().getStatusCode()) {
            throw new Exception("Http response error: " + response.getStatusLine()
                    + "\n " + inputStreamToStr(response.getEntity().getContent()));
        }

    }

    public static String inputStreamToStr(InputStream in) {
        if (null == in) {
            return null;
        }

        StringBuffer strBuf = new StringBuffer("");
        BufferedReader bufferedReader = null;

        try {
            bufferedReader = new BufferedReader(new InputStreamReader(in));
            String lineContent = bufferedReader.readLine();
            while (lineContent != null) {
                strBuf.append(lineContent);
                lineContent = bufferedReader.readLine();
            }
        } catch (IOException e) {
            System.out.println("Exception: " + e.getMessage());
        } finally {
            if (null != bufferedReader) {
                try {
                    bufferedReader.close();
                } catch (IOException ignore) {
                    // to do nothing.
                }
            }
        }
        return strBuf.toString();
    }
}
