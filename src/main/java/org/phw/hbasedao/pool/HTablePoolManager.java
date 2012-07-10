package org.phw.hbasedao.pool;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

/**
 * HTable Pool 管理器。
 * @author BingooHuang
 *
 */
public class HTablePoolManager {
    public static final String DEFAULT_INSTANCE = "default";
    private static volatile HashMap<String, HTablePool> poolCache = new HashMap<String, HTablePool>();
    private static volatile HashMap<String, Configuration> confCache = new HashMap<String, Configuration>();

    /**
     * 创建HBase实例。
     * @param hbaseInstanceName 实例名称。
     * @param quorum hbase.zookeeper.quorum
     * @param clientPort hbase.zookeeper.property.clientPort
     * @return Configuration
     */
    public static Configuration createHBaseConfiguration(String hbaseInstanceName, String quorum, String clientPort) {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", quorum);
        configuration.set("hbase.zookeeper.property.clientPort", clientPort);
        synchronized (confCache) {
            confCache.put(hbaseInstanceName, configuration);
        }

        return configuration;
    }

    public static Configuration getHBaseConfiguration() {
        return getHBaseConfiguration(DEFAULT_INSTANCE);
    }

    public static Configuration getHBaseConfiguration(String hbaseInstanceName) {
        Configuration hBaseConfiguration = confCache.get(hbaseInstanceName);
        if (hBaseConfiguration == null && DEFAULT_INSTANCE.equals(hbaseInstanceName)) {
            return createHBaseConfiguration(DEFAULT_INSTANCE, "127.0.0.1", "2181");
        }

        return hBaseConfiguration;
    }

    /**
     * 取得默认的HTable池。
     * @return HTablePool
     */
    public static HTablePool getHTablePool() {
        Configuration hBaseConfiguration = getHBaseConfiguration(DEFAULT_INSTANCE);
        if (hBaseConfiguration == null) {
            hBaseConfiguration = createHBaseConfiguration(DEFAULT_INSTANCE, "127.0.0.1", "2181");
        }

        return getHTablePool(DEFAULT_INSTANCE, 100);
    }

    /**
     * 取得池。
     * @param hbaseInstanceName Hbase实例名称。
     * @return HTablePool
     */
    public static HTablePool getHTablePool(String hbaseInstanceName, int maxSize) {
        HTablePool hTablePool = poolCache.get(hbaseInstanceName);
        if (hTablePool != null) {
            return hTablePool;
        }

        synchronized (poolCache) {
            hTablePool = poolCache.get(hbaseInstanceName);
            if (hTablePool != null) {
                return hTablePool;
            }

            hTablePool = new HTablePoolEnhanced(getHBaseConfiguration(hbaseInstanceName), maxSize);
            poolCache.put(hbaseInstanceName, hTablePool);
        }

        return hTablePool;
    }

    /**
     * 取得HTable对象。
     * @param tableName 表名。
     * @return HTableInterface
     */
    public static HTableInterface getHTable(String tableName) {
        return getHTablePool().getTable(tableName);
    }

    /**
     * 取得HTable对象。
     * @param tableName 表名。
     * @param hbaseInstanceName Hbase实例名称。
     * @return HTableInterface
     */
    public static HTableInterface getHTable(String tableName, String hbaseInstanceName) {
        return getHTablePool(hbaseInstanceName, 100).getTable(tableName);
    }

}
