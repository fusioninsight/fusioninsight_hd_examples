业务场景：
一个mapreduce应用从HDFS中读取数据，做分析后，将分析结果写入到solr，并存储到HDFS中。

前提：
1.申请一个业务账号，具有访问solr指定collection的权限，也具有访问HDFS数据的权限，具有提交应用到yarn运行的权限（某个队列）
2.待分析的数据已经放在某个HDFS路径下，例如“/user/tester1/mapreduce/input”；
3.安装了FusionInsight HD客户端，包含了YARN、HDFS、Solr等组件。

参考《应用开发指南》->"安全模式"->"MapReduce开发指南"->"调测程序"->"在Linux环境中运行程序"->"编译并运行程序"
1.完成应用程序的jar包编译后，将jar包放在某个路径下，例如：/opt/zhuojunjian/jar，jar包为mrToSolr-C70.jar。
2.将reduce阶段认证所需的配置文件，包括user.keytab文件、krb5.conf、jaas.conf配置文件（本示例的名称为“jaas_mr.conf”）等，放在某个路径下，例如：/opt/zhuojunjian/conf；
其中，jaas.conf的内容，参考指南中的示例。
3.提交应用给yarn时，通过kinit完成认证，例如：
kinit -kt /opt/zhuojunjian/conf/user.keytab tester1
4.export所需的配置信息和业务jar包、本应用引入的依赖jar包，例如：
export YARN_USER_CLASSPATH=/opt/zhuojunjian/conf/:/opt/zhuojunjian/hadoopclient/Solr/lib/*:/opt/zhuojunjian/jar/*
5.提交应用：
yarn jar mrToSolr-C70.jar com.huawei.bigdata.mapreduce.examples.MapReduceToSolr /user/tester1/mapreduce/input /user/tester1/mapreduce/output

