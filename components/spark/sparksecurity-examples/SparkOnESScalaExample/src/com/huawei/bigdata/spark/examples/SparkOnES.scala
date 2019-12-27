package com.huawei.bigdata.spark.examples

import com.huawei.bigdata.hadoop.security.LoginUtil
import org.apache.hadoop.conf.Configuration
import org.apache.http.Header
import org.apache.http.HttpHost
import org.apache.http.HttpStatus
import org.apache.http.client.config.RequestConfig
import org.apache.http.entity.ContentType
import org.apache.http.message.BasicHeader
import org.apache.http.nio.entity.NStringEntity
import org.apache.http.util.EntityUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.client.Response
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestClientBuilder
import org.slf4j.LoggerFactory
import java.io.File
import java.io.FileInputStream
import java.util.{Collections, Properties}

import scala.collection.mutable.ListBuffer

class SparkOnES {
  private val LOG = LoggerFactory.getLogger(classOf[SparkOnES])
  private var isSecureMode : String = null
  private var esServerHost : String = null
  private var MaxRetryTimeoutMillis : Int = 0
  private var index : String = null
  private var dataType : String = null
  private var id : Int = 0
  private var shardNum : Int = 0
  private var replicaNum : Int = 0
  private var ConnectTimeout : Int = 0
  private var SocketTimeout : Int = 0
  private var schema : String= "https"
  private var restClient : RestClient= null
  private var principal : String = null

  /*
   * initialize basic configurations for elasticsearch
   * */ @throws[Exception]
  private def initProperties() = {
    val properties = new Properties
    var path = System.getProperty("user.dir") + File.separator + "es-example.properties"
    path = path.replace("\\", "\\\\")
    try
      properties.load(new FileInputStream(new File(path)))
    catch {
      case e: Exception =>
        throw new Exception("Failed to load properties file : " + path)
    }
    //EsServerHost in es-example.properties must as ip1:port1,ip2:port2,ip3:port3....  eg:1.1.1.1:24100,2.2.2.2:24102,3.3.3.3:24100
    esServerHost = properties.getProperty("EsServerHost")
    MaxRetryTimeoutMillis = Integer.valueOf(properties.getProperty("MaxRetryTimeoutMillis"))
    ConnectTimeout = Integer.valueOf(properties.getProperty("ConnectTimeout"))
    SocketTimeout = Integer.valueOf(properties.getProperty("SocketTimeout"))
    isSecureMode = properties.getProperty("isSecureMode")
    dataType = properties.getProperty("type")
    id = Integer.valueOf(properties.getProperty("id"))
    shardNum = Integer.valueOf(properties.getProperty("shardNum"))
    replicaNum = Integer.valueOf(properties.getProperty("replicaNum"))
    principal = properties.getProperty("principal")
    LOG.info("EsServerHost:" + esServerHost)
    LOG.info("MaxRetryTimeoutMillis:" + MaxRetryTimeoutMillis)
    LOG.info("ConnectTimeout:" + ConnectTimeout)
    LOG.info("SocketTimeout:" + SocketTimeout)
    LOG.info("index:" + index)
    LOG.info("shardNum:" + shardNum)
    LOG.info("replicaNum:" + replicaNum)
    LOG.info("isSecureMode:" + isSecureMode)
    LOG.info("dataType:" + dataType)
    LOG.info("id:" + id)
    LOG.info("principal:" + principal)
  }

  /**
    * Get hostArray by esServerHost of cluster in property file
    *
    * @param esServerHost
    * @return
    */
  @throws[Exception]
  def getHostArray(esServerHost: String): Array[HttpHost] = {
    if ("false" == isSecureMode) schema = "http"
    var hosts : List[HttpHost] = List()
    val hostArr = esServerHost.split(",")
    for (host <- hostArr) {
      val ipPort = host.split(":")
      val hostNew = new HttpHost(ipPort(0), Integer.valueOf(ipPort(1)), schema)
      hosts = hostNew :: hosts
    }
    Array(hosts:_*)
  }

  /**
    * Get one rest client instance.
    *
    * @return
    * @throws Exception
    */
  @throws[Exception]
  private def getRestClientBuilder(HostArray: Array[HttpHost]) = {
    var builder : RestClientBuilder = null
    if (isSecureMode == "true") {
      System.setProperty("es.security.indication", "true")
      System.setProperty("elasticsearch.kerberos.jaas.appname", "EsClient")
      builder = RestClient.builder(HostArray:_*)
    }
    else {
      System.setProperty("es.security.indication", "false")
      builder = RestClient.builder(HostArray:_*)
    }
    val defaultHeaders = Array[Header](new BasicHeader("Accept", "application/json"), new BasicHeader("Content-type", "application/json"))
    builder.setDefaultHeaders(defaultHeaders)
    builder.setMaxRetryTimeoutMillis(MaxRetryTimeoutMillis)
    builder.setFailureListener(new RestClient.FailureListener() {
      def onFailure(host: HttpHost): Unit = {
        //trigger some actions when failure occurs
      }
    })
    builder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
      override def customizeRequestConfig(requestConfigBuilder: RequestConfig.Builder): RequestConfig.Builder = requestConfigBuilder.setConnectTimeout(ConnectTimeout).setSocketTimeout(SocketTimeout)
    })
    builder
  }

  @throws[Exception]
  private def getRestClient(HostArray: Array[HttpHost]) = {
    var restClient : RestClient = null
    restClient = getRestClientBuilder(HostArray).build
    LOG.info("The RestClient has been created !")
    restClient.setHosts(HostArray: _*)
    restClient
  }

  /**
    * Check whether the index has already existed in ES
    *
    * @param index
    **/
  def exist(index: String): Boolean = {
    var response : Response = null
    val params = Collections.singletonMap("pretty", "true")
    try {
      response = restClient.performRequest("HEAD", "/" + index, params)
      if (HttpStatus.SC_OK == response.getStatusLine.getStatusCode) {
        LOG.info("Ths index exist already:" + index)
        return true
      }
      if (HttpStatus.SC_NOT_FOUND == response.getStatusLine.getStatusCode) {
        LOG.info("Index is not exist:" + index)
        return false
      }
    } catch {
      case e: Exception =>
        LOG.error("Fail to check index!", e)
    }
    false
  }

  /**
    * Create one index with shard number.
    *
    * @param index
    * @throws Exception
    */
  private def createIndexWithShardNum(index: String) = {
    var rsp : Response = null
    val params = Collections.singletonMap("pretty", "true")
    val jsonString = "{" + "\"settings\":{" + "\"number_of_shards\":\"" + shardNum + "\"," + "\"number_of_replicas\":\"" + replicaNum + "\"" + "}}"
    val entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON)
    try {
      rsp = restClient.performRequest("PUT", "/" + index, params, entity)
      if (HttpStatus.SC_OK == rsp.getStatusLine.getStatusCode) LOG.info("CreateIndexWithShardNum successful.")
      else LOG.error("CreateIndexWithShardNum failed.")
      LOG.info("CreateIndexWithShardNum response entity is : " + EntityUtils.toString(rsp.getEntity))
    } catch {
      case e: Exception =>
        LOG.error("CreateIndexWithShardNum failed, exception occurred.", e)
    }
  }

  /**
    * Put one document into the index
    *
    * @param index
    * @param jsonString
    */
  private def putData(index: String, dataType: String, id: String, jsonString: String) = {
    val params = Collections.singletonMap("pretty", "true")
    val entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON)
    var rsp : Response = null
    try {
      rsp = restClient.performRequest("POST", "/" + index + "/" + dataType + "/" + id, params, entity)
      if (HttpStatus.SC_OK == rsp.getStatusLine.getStatusCode || HttpStatus.SC_CREATED == rsp.getStatusLine.getStatusCode) LOG.info("PutData successful.")
      else LOG.error("PutData failed.")
      LOG.info("PutData response entity is : " + EntityUtils.toString(rsp.getEntity))
    } catch {
      case e: Exception =>
        LOG.error("PutData failed, exception occurred.", e)
    }
  }

  /**
    * Query all data of one index.
    *
    * @param index
    * @param dataType
    * @param id :the id of the people for query
    */
  private def queryData(index: String, dataType: String, id: String) = {
    var rsp : Response = null
    val params = Collections.singletonMap("pretty", "true")
    try {
      rsp = restClient.performRequest("GET", "/" + index + "/" + dataType + "/" + id, params)
      if (HttpStatus.SC_OK == rsp.getStatusLine.getStatusCode) LOG.info("QueryData successful.")
      else LOG.error("QueryData failed.")
      val result = EntityUtils.toString(rsp.getEntity)
      LOG.info("QueryData response entity is : " + result)
      result
    } catch {
      case e: Exception =>
        LOG.error("QueryData failed, exception occurred.", e)
        ""
    }
  }

  /**
    * Delete one index
    *
    * @param index
    */
  private def delete(index: String) = {
    var rsp : Response= null
    try {
      rsp = restClient.performRequest("DELETE", "/" + index + "?&pretty=true")
      if (HttpStatus.SC_OK == rsp.getStatusLine.getStatusCode) LOG.info("Delete successful.")
      else LOG.error("Delete failed.")
      LOG.info("Delete response entity is : " + EntityUtils.toString(rsp.getEntity))
    } catch {
      case e: Exception =>
        LOG.error("Delete failed, exception occurred.", e)
    }
  }
}

object SparkOnES{
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val sparkOnES : SparkOnES = new SparkOnES
    sparkOnES.initProperties
    println("initialize property file succeed!")
    //login with keytab
    val userPrincipal = "super"
    val userKeytabPath = System.getProperty("user.dir") + File.separator + "user.keytab"
    val krb5ConfPath = System.getProperty("user.dir") + File.separator + "krb5.conf"
    //generate jaas file, which is in the same dir with keytab file
    LoginUtil.setJaasFile(userPrincipal, userKeytabPath)
    val hadoopConf = new Configuration
    LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf)
    try {
      sparkOnES.restClient = sparkOnES.getRestClient(sparkOnES.getHostArray(sparkOnES.esServerHost))
      sparkOnES.index = "people"
      sparkOnES.dataType = "doc"
      //check whether the index to be added has already exist, remove the exist one
      if (sparkOnES.exist(sparkOnES.index)) sparkOnES.delete(sparkOnES.index)
      sparkOnES.createIndexWithShardNum(sparkOnES.index)
      val conf = new SparkConf().setAppName("SparkOnES")
      val sc = new SparkContext(conf)
      val inputs = sc.textFile("/spark-on-es/people.json")
      val jsonList = inputs.collect
      var i = 0
      jsonList.map(jsonStr => {
        sparkOnES.putData(sparkOnES.index, sparkOnES.dataType, String.valueOf(i), jsonStr); i +=1
      })

      val resultList = ListBuffer[String]()
      i = 0
      while ({
        i < jsonList.size
      }) {
        resultList.append(sparkOnES.queryData(sparkOnES.index, sparkOnES.dataType, String.valueOf(i)))
        i+=1
      }
      val resultRDD = sc.parallelize(resultList)
      val count = resultRDD.count
      System.out.println("The total count is:" + count)
    } catch {
      case e: Exception =>
        sparkOnES.LOG.error("There are exceptions in main.", e)
    } finally if (sparkOnES.restClient != null) try {
      sparkOnES.restClient.close()
      sparkOnES.LOG.info("Close the client successful in main.")
    } catch {
      case e1: Exception =>
        sparkOnES.LOG.error("Close the client failed in main.", e1)
    }
  }
}
