package com.huawei.storm.example.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.solr.bolt.SolrUpdateBolt;
import org.apache.storm.solr.config.SolrCommitStrategy;
import org.apache.storm.solr.config.SolrConfig;
import org.apache.storm.solr.mapper.SolrJsonMapper;
import org.apache.storm.solr.mapper.SolrMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SimpleSolrJsonTopology
{
    protected static String COLLECTION = "gettingstarted";
    
    private static final String SECURITY_AUTO_KEYTAB_PLUGIN =
        "org.apache.storm.security.auth.kerberos.AutoTGTFromKeytab";
    
    /**
     * Zookeeper连接串，格式：ip1:port,ip2:port.../solr
     */
    private static final String ZOOKEEPER_CONNECT_STR = "{ip}:{port}/solr";
    
    public void run(String[] args)
        throws Exception
    {
        final StormTopology topology = getTopology();
        Config config = new Config();
        config.setDebug(true);
        // 配置安全插件
        setSecurityPlugin(config);
        if (args.length >= 2)
        {
            //用户更改了默认的keytab文件名，这里需要将新的keytab文件名通过参数传入
            config.put(Config.TOPOLOGY_KEYTAB_FILE, args[1]);
        }
        //命令行提交拓扑
        StormSubmitter.submitTopologyWithProgressBar(args[0], config, topology);
        
    }
    
    private static void setSecurityPlugin(Config conf)
    {
        // 增加kerberos认证所需的plugin到列表中，安全模式必选
        List<String> auto_tgts = new ArrayList<String>();
        // 当前只支持keytab方式
        auto_tgts.add(SECURITY_AUTO_KEYTAB_PLUGIN);
        // 将端配置的plugin列表写入config指定项中，安全模式必配
        conf.put(Config.TOPOLOGY_AUTO_CREDENTIALS, auto_tgts);
    }
    
    protected static SolrConfig getSolrConfig()
    {
        String zkHostString = ZOOKEEPER_CONNECT_STR; 
        return new SolrConfig(zkHostString);
    }
    
    protected static SolrClient getSolrClient()
    {
        String zkHostString = ZOOKEEPER_CONNECT_STR;
        return new CloudSolrClient(zkHostString);
    }
    
    public static void main(String[] args)
        throws Exception
    {
        SimpleSolrJsonTopology solrJsonTopology = new SimpleSolrJsonTopology();
        solrJsonTopology.run(args);
    }
    
    protected SolrMapper getSolrMapper() throws IOException {
        final String jsonTupleField = "JSON";
        return new SolrJsonMapper.Builder(COLLECTION, jsonTupleField).build();
    }
    
    protected SolrCommitStrategy getSolrCommitStgy()
    {
        return null;
    }
    
    protected StormTopology getTopology() throws IOException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("SolrJsonSpout", new SolrJsonSpout());
        builder.setBolt("SolrUpdateBolt", new SolrUpdateBolt(getSolrConfig(), getSolrMapper(), getSolrCommitStgy()))
                .shuffleGrouping("SolrJsonSpout");
        return builder.createTopology();
    }
    
}
