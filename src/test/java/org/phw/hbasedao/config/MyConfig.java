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
        config.put("hbase.zookeeper.quorum", "127.0.0.1");
        config.put("hbase.zookeeper.property.clientPort", "2181");
        config.put("zookeeper.session.timeout", "180000");

        return config;
    }
}
