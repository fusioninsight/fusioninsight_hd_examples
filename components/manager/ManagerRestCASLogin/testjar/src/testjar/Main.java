package testjar;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;

//import com.huawei.hadoop.om.rest.login.CasLogin;
import com.huawei.hadoop.om.rest.operation.ModifyPassword;
import org.apache.http.client.HttpClient;
import org.apache.http.entity.mime.content.FileBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.huawei.hadoop.om.rest.exception.InvalidInputParamException;
import com.huawei.hadoop.om.rest.operation.HttpManager;
import com.huawei.hadoop.om.rest.validutil.ParamsValidUtil;

public class Main
{
    
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    
    private static final String DEFAULT_SSL_VERSION = "TLSv1.2";
    
    private static final String DEFAULT_PASSWORD = "Admin@123";

    /**
     * 程序运行入口
     * @param args
     */
    
    public static void main(String[] args)
    {
        LOG.info("Enter main.");
        
        //文件UserInfo.properties的路径
        String userFilePath = "./conf/UserInfo.properties";
        InputStream userInfoInputStream = null;
        ResourceBundle resourceBundle = null;
        try
        {
            File file = new File(userFilePath);
            if (!file.exists())
            {
                LOG.error("The user info file doesn't exist.");
                return;
            }
            LOG.info("Get the web info and user info from file {} ", file.getAbsolutePath());
            
            userInfoInputStream = new BufferedInputStream(new FileInputStream(userFilePath));
            resourceBundle = new PropertyResourceBundle(userInfoInputStream);
            
            //获取集群和用户信息
            String webUrl = resourceBundle.getString("webUrl");
            String casUrl = resourceBundle.getString("casUrl");
            String userName = resourceBundle.getString("userName");
            String password = resourceBundle.getString("password");
            String keytabFilePath = resourceBundle.getString("keytabFilePath");
            String userTLSVersion = resourceBundle.getString("userTLSVersion");

            if (ParamsValidUtil.isEmpty(webUrl, casUrl, userName))
            {
                LOG.error("Invalid input param.");
                throw new InvalidInputParamException("Invalid input param.");
            }
            LOG.info("The username is {}, casUrl is {}, webUrl is {}.", userName, casUrl, webUrl);
            
            if (ParamsValidUtil.isEmpty(password))
            {
                password = DEFAULT_PASSWORD;
            }
            
            if (ParamsValidUtil.isEmpty(userTLSVersion))
            {
                userTLSVersion = DEFAULT_SSL_VERSION;
            }
            
            //调用登录接口完成登录认证
            LOG.info("Ready to login cas.");
            
            //以password方式登录
          HttpClient httpClient = CasLogin.usernamePasswordLogin(casUrl, webUrl, userName, password, userTLSVersion);

            //C80及以后不建议使用  keytab方式登录认证
//            LOG.info("The keytabFilePath is {}",keytabFilePath);
         //   HttpClient httpClient = CasLogin.usernameKeytabLogin(casUrl, webUrl, userName, keytabFilePath, password, userTLSVersion);
       //     HttpClient httpClient = CasLogin.usernameKeytabLogin(casUrl, webUrl, userName, keytabFilePath);;

            if (httpClient == null)
            {
                LOG.error("The httpClient is empty.");
                return;
            }
            LOG.info("Login successfully.");
            
//            //使用示例，发送HTTP POST请求，添加用户
//            LOG.info("Ready to add user.");
//            String operationUrl = webUrl + "/access/user/users.do";
//            String jsonFilePath = "./conf/addUser.json";
//            HttpManager.sendHttpPostRequest(httpClient, operationUrl, jsonFilePath);
//            LOG.info("Add user finished.");
//
            //使用示例，发送HTTP GET请求，查询用户列表
            LOG.info("Ready to query user list.");
            String queryUserList = webUrl + "/access/user/users.do?offset=0&limit=200&defaultuser=1&mode=monitoring";
            HttpManager.sendHttpGetRequest(httpClient, queryUserList);
            LOG.info("Query user list finished.");


//            //使用示例，发送HTTP POST请求，重置密码
//            LOG.info("resetpasswd.");
//            String resetPasswdOperationUrl = webUrl + "/access/user/users/password/resetpasswd.do";
//            String resetPasswdJsonFilePath = "./conf/resetPassword.json";
//            HttpManager.sendHttpPostRequest(httpClient, resetPasswdOperationUrl, resetPasswdJsonFilePath);
//            LOG.info("Add user finished.");
//
//            //使用示例，发送HTTP POST请求，修改密码
//            LOG.info("modifyPassword.");
//            String modifyPasswdOperationUrl = webUrl + "/access/modify_password.htm";
//            Map<String,String>  map = new HashMap<>();
//            map.put("username","testUsr1");
//            map.put("oldPassword","Admin@123");
//            map.put("newPassword","Huawei@1234");
//            map.put("confirmPassword","Huawei@1234");
//            HttpManager.sendHttpPostRequestWithForm(httpClient, modifyPasswdOperationUrl, map);
//            LOG.info("Add user finished.");

        }
        catch (FileNotFoundException e)
        {
            LOG.error("File not found exception.");
        }
        catch (IOException e)
        {
            LOG.error("IOException.");
        }
        catch (Throwable e)
        {
            System.out.println("e.getMessage()"+e.getMessage());
            System.out.println("e.getCause()"+e.getCause());
            LOG.warn("Throwable Exception.");
        }
        finally
        {
            if (userInfoInputStream != null)
            {
                try
                {
                    userInfoInputStream.close();
                }
                catch (IOException e)
                {
                    LOG.error("IOException.");
                }
            }
            
        }
        LOG.info("Exit main.");
        
    }
}
