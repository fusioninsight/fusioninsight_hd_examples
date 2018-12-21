package main.scala.com.zr.police

import java.io.{FileInputStream, IOException}
import java.util.Properties

import org.apache.solr.client.solrj.{SolrQuery, SolrServerException}
import org.apache.solr.client.solrj.request.CollectionAdminRequest
import org.apache.solr.client.solrj.response.CollectionAdminResponse
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util

import main.scala.com.zr.utils.LoginUtil
import org.apache.hadoop.conf.Configuration
import org.apache.solr.client.solrj.SolrServerException
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.rdd.RDD

object SolrSearch {
  val LOG = LoggerFactory.getLogger("SolrSearch")
  //读取配置文件
  val properties = new Properties()
  val in = this.getClass.getClassLoader().getResourceAsStream("solr-example.properties")
  //    val path = System.getProperty("user.dir") + File.separator + "main/resource" + File.separator + "Fproducer.properties"
  properties.load(in)

  //连接solr客户端
  @throws[SolrException]
  def getCloudSolrClient(): CloudSolrClient = {
    val zkHost = properties.getProperty("zkHost")
    val zkClientTimeout = properties.getProperty("zkClientTimeout").toInt
    val zkConnectTimeout = properties.getProperty("zkConnectTimeout").toInt
    val builder = new CloudSolrClient.Builder()
    builder.withZkHost(zkHost)
    val cloudSolrClient = builder.build
    cloudSolrClient.setZkClientTimeout(zkClientTimeout)
    cloudSolrClient.setZkConnectTimeout(zkConnectTimeout)
    cloudSolrClient.connect
    LOG.info("The cloud Server has been connected !!!!")
    val zkStateReader = cloudSolrClient.getZkStateReader
    val cloudState = zkStateReader.getClusterState
    LOG.info("The zookeeper state is : {}", cloudState)
    return cloudSolrClient
  }

  //创建索引
  def createCollection(collectionName:String): Unit = {
    val cloudSolrClient: CloudSolrClient = getCloudSolrClient()
    val COLLECTION_NAME = collectionName
    val DEFAULT_CONFIG_NAME = properties.getProperty("DEFAULT_CONFIG_NAME")
    val shardNum = properties.getProperty("shardNum").toInt
    val replicaNum = properties.getProperty("replicaNum").toInt
    val create = CollectionAdminRequest.createCollection(COLLECTION_NAME, DEFAULT_CONFIG_NAME, shardNum, replicaNum)
    var response: CollectionAdminResponse = null
    try {
      response = create.process(cloudSolrClient)
    }
    catch {
      case e: SolrServerException =>
        LOG.error("Failed to create collection", e)
        throw new SolrException("Failed to create collection")
      case e: IOException =>
        LOG.error("Failed to create collection", e)
        throw new SolrException("Failed to create collection")
      case e: Exception =>
        LOG.error("Failed to create collection", e)
        throw new SolrException("unknown exception")
    }
    if (response.isSuccess) LOG.info("Success to create collection[{}]", COLLECTION_NAME)
    else {
      throw new SolrException("Failed to create collection")
    }
  }

  //查询所有索引
  def queryAllCollections(cloudSolrClient: CloudSolrClient): List[String] = {
    val list: CollectionAdminRequest.List = new CollectionAdminRequest.List()
    var listRes: CollectionAdminResponse = null
    try {
      listRes = list.process(cloudSolrClient)
    } catch {
      case e@(_: SolrServerException | _: IOException) =>
        LOG.error("Failed to list collection", e)
        throw new SolrException("Failed to list collection")
      case e: Exception =>
        LOG.error("Failed to list collection", e)
        throw new SolrException("unknown exception")
    }
    val collectionNames: List[String] = listRes.getResponse.get("collections").asInstanceOf
    LOG.info("All existed collections : {}", collectionNames)
    return collectionNames
  }

  //添加索引内容
  @throws[SolrException]
  def addDocs(cloudSolrClient: CloudSolrClient, name: String, addr: String, date: String, id: String): Unit = {
    val documents = new util.ArrayList[SolrInputDocument]
    val doc = new SolrInputDocument()
    doc.addField("id", id)
    doc.addField("name", name)
    doc.addField("address", addr)
    doc.addField("date", date)
    documents.add(doc)
    try {
      cloudSolrClient.add(documents)
      LOG.info("success to add index")
    } catch {
      case e: SolrServerException =>
        LOG.error("Failed to add document to collection", e)
        throw new SolrException("Failed to add document to collection")
      case e: IOException =>
        LOG.error("Failed to add document to collection", e)
        throw new SolrException("Failed to add document to collection")
      case e: Exception =>
        LOG.error("Failed to add document to collection", e)
        throw new SolrException("unknown exception")
    }
  }

  //索引查询
  @throws[SolrException]
  def queryIndex(cloudSolrClient: CloudSolrClient, queryCondition: String): Unit = {
    val query = new SolrQuery()
    query.setQuery(queryCondition)
    try {
      val response = cloudSolrClient.query(query)
      val docs = response.getResults
      LOG.info("Query wasted time : {}ms", response.getQTime)
      LOG.info("Total doc num : {}", docs.getNumFound)
      import scala.collection.JavaConversions._
      for (doc <- docs) {
        LOG.info("doc detail : " + doc.getFieldValueMap)
      }
    } catch {
      case e: SolrServerException =>
        LOG.error("Failed to query document", e)
        throw new SolrException("Failed to query document")
      case e: IOException =>
        LOG.error("Failed to query document", e)
        throw new SolrException("Failed to query document")
      case e: Exception =>
        LOG.error("Failed to query document", e)
        throw new SolrException("unknown exception")
    }
  }
}

