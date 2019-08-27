1. 通过Fusioninsight Manager下载客户端，只需要下载配置文件
   将解压后，将FusionInsight_Services_ClientConfig_Config\HDFS\config中的core-site.xml和hdfs-site.xml文件复制到Fusioninsight_HD_Examples\components\hdfs\src\main\resources\conf\目录下
   这两个配置文件是客户端与服务端的对接信息，主要包含NameNode等IP地址信息
2. 通过Fusioninsight Manager的用户管理界面下载用户认证配置，包括user.keytab（用户认证凭据）和krb5.conf（kerberos服务端对接配置）
   解压后放到Fusioninsight_HD_Examples\components\hdfs\src\main\resources\conf\
3. 样例中的log4j.properties仅供样例代码使用，实际项目需要自己提供