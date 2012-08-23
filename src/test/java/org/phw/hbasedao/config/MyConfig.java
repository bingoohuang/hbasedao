package org.phw.hbasedao.config;

import java.util.HashMap;
import java.util.Map;

import org.phw.hbasedao.annotations.HConnectionConfig;
import org.phw.hbasedao.pool.HTablePoolManager;

@HConnectionConfig
public class MyConfig {

    @HConnectionConfig(HTablePoolManager.DEFAULT_INSTANCE)
    public Map<String, String> createConfig() {
        Map<String, String> config = new HashMap<String, String>();
        config.put("hbase.zookeeper.quorum", "10.142.195.67,10.142.151.88,10.142.195.63");
        config.put("hbase.zookeeper.property.clientPort", "2181");
        config.put("zookeeper.session.timeout", "180000");
        config.put("hhbase.table.references.max", "10");

        return config;
    }
}
