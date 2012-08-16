package org.phw.hbasedao.util;

import java.util.Map;

import org.junit.Test;
import org.phw.hbasedao.pool.HTablePoolManager;

import static org.junit.Assert.*;

public class ConfigUtilsTest {

    @Test
    public void testGetConfig() {
        Map<String, String> config = ConfigUtils.getConfig(HTablePoolManager.DEFAULT_INSTANCE);
        assertEquals("{zookeeper.session.timeout=180000, hbase.zookeeper.property.clientPort=2181, " +
                "hbase.zookeeper.quorum=127.0.0.1}", config.toString());
    }

}
