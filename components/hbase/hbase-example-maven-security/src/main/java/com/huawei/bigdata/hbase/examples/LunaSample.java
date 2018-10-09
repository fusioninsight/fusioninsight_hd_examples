package com.huawei.bigdata.hbase.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.luna.client.ColumnField;
import org.apache.luna.client.LunaAdmin;
import org.apache.luna.client.Mapping;
import org.apache.luna.filter.FullTextFilter;
import org.apache.luna.mapreduce.BuildCollection;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.Create;

import com.huawei.bigdata.security.LoginUtilForSolr;

public class LunaSample
{

    private final static Log LOG = LogFactory.getLog(LunaSample.class.getName());

    private static Configuration conf;

    private static final TableName HBASE_TABLE = TableName.valueOf("testTable");

    private static final String COLLECTION_NAME = "testCollection";

    private static final byte[] TABLE_FAMILY = Bytes.toBytes("f");

    private static final String CONFSET_NAME = "confWithHBase";

    private static final int NUM_OF_SHARDS = 3;

    private static final int NUM_OF_REPLICATIONFACTOR = 2;

    public static void main(String[] args) throws Exception
    {
        try
        {
            if (1 == args.length)
            {
                conf = HBaseConfiguration.create();
            }
            else
            {
                conf = LoginUtilForSolr.login();
            }
            testFullTextScan();
        }
        catch (IOException e)
        {
            LOG.error("Failed to run luna sample.", e);
        }
        System.exit(0);
    }

    static class AdminSingleton
    {
        private static LunaAdmin admin;

        public synchronized LunaAdmin getAdmin() throws IOException
        {
            if (null == admin)
            {
                admin = new LunaAdmin(conf);
            }
            return admin;
        }

    }

    public static void testFullTextScan() throws Exception
    {
        /**
         * Create create request of Solr. Specify collection name, confset name, number of shards, and number of
         * replication factor.
         */
        Create create = new Create();
        create.setCollectionName(COLLECTION_NAME);
        create.setConfigName(CONFSET_NAME);
        create.setNumShards(NUM_OF_SHARDS);
        create.setReplicationFactor(NUM_OF_REPLICATIONFACTOR);

        /**
         * Create mapping. Specify index fields(mandatory) and non-index fields(optional).
         */
        List<ColumnField> indexedFields = new ArrayList<ColumnField>();
        indexedFields.add(new ColumnField("name", "f:n"));
        indexedFields.add(new ColumnField("cat", "f:t"));
        indexedFields.add(new ColumnField("features", "f:d"));
        Mapping mapping = new Mapping(indexedFields);

        /**
         * Create table descriptor of HBase.
         */
        HTableDescriptor desc = new HTableDescriptor(HBASE_TABLE);
        desc.addFamily(new HColumnDescriptor(TABLE_FAMILY));

        /**
         * Create table and collection at the same time.
         */
        LunaAdmin admin = null;
        try
        {
            admin = new AdminSingleton().getAdmin();
            admin.deleteTable(HBASE_TABLE);
            if (!admin.tableExists(HBASE_TABLE))
            {
                admin.createTable(desc, Bytes.toByteArrays(new String[] { "0", "1", "2", "3", "4" }), create, mapping);
            }

            /**
             * Put data.
             */
            Table table = admin.getTable(HBASE_TABLE);
            int i = 0;
            while (i < 5)
            {
                byte[] row = Bytes.toBytes(i + "+sohrowkey");
                Put put = new Put(row);
                put.addColumn(TABLE_FAMILY, Bytes.toBytes("n"), Bytes.toBytes("ZhangSan" + i));
                put.addColumn(TABLE_FAMILY, Bytes.toBytes("t"), Bytes.toBytes("CO" + i));
                put.addColumn(TABLE_FAMILY, Bytes.toBytes("d"), Bytes.toBytes("Male, Leader of M.O" + i));
                table.put(put);
                i++;
            }

            admin.getSolrClient().getZkStateReader().updateClusterState();
            /**
             * Scan table.
             */
            Scan scan = new Scan();
            SolrQuery query = new SolrQuery();
            query.setQuery("name:ZhangSan1 AND cat:CO1");
            Filter filter = new FullTextFilter(query, COLLECTION_NAME);
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            LOG.info("-----------------records----------------");
            for (Result r = scanner.next(); r != null; r = scanner.next())
            {
                for (Cell cell : r.rawCells())
                {
                    LOG.info(Bytes.toString(CellUtil.cloneRow(cell)) + ":" + Bytes.toString(CellUtil.cloneFamily(cell))
                            + "," + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
                            + Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            LOG.info("-------------------end------------------");

            /**
             * Delete collection.
             */
            admin.deleteCollection(HBASE_TABLE, COLLECTION_NAME);

            /**
             * Add collection async.
             */
            create.setCollectionName(COLLECTION_NAME + "tra");
            List<ColumnField> nonIndexedFields = new ArrayList<ColumnField>();
            nonIndexedFields.add(new ColumnField("manu_exact", "f:m"));
            mapping.setNonIndexFields(nonIndexedFields);
            admin.addCollectionAsync(HBASE_TABLE, create, mapping);

            admin.getSolrClient().getZkStateReader().updateClusterState();

            /**
             * Reload data to collection and enable collection. Can only run on Linux.
             */
            if (!System.getProperty("os.name").startsWith("Windows"))
            {
                Job job = BuildCollection.createSubmittableJob(conf,
                        new String[] { HBASE_TABLE.getNameAsString(), COLLECTION_NAME + "tra" });
                if (job.waitForCompletion(true))
                {
                    admin.enableCollection(HBASE_TABLE, COLLECTION_NAME + "tra");
                }
                query.setQuery("name:ZhangSan1 AND cat:CO1");
                filter = new FullTextFilter(query, COLLECTION_NAME + "tra");
                scan.setFilter(filter);
                ResultScanner scanner1 = table.getScanner(scan);
                LOG.info("-----------------records----------------");
                for (Result r = scanner1.next(); r != null; r = scanner1.next())
                {
                    for (Cell cell : r.rawCells())
                    {
                        LOG.info(Bytes.toString(CellUtil.cloneRow(cell)) + ":"
                                + Bytes.toString(CellUtil.cloneFamily(cell)) + ","
                                + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
                                + Bytes.toString(CellUtil.cloneValue(cell)));
                    }
                }
                LOG.info("-------------------end------------------");
            }

            /**
             * Delete table.
             */
            admin.deleteTable(HBASE_TABLE);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        finally
        {
            /**
             * When everything done, close LunaAdmin.
             */
            admin.close();
        }
    }

}
