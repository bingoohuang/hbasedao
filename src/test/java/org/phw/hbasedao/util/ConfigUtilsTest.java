package org.phw.hbasedao.util;

import java.util.Map;

import org.junit.Test;
import org.phw.hbasedao.pool.HTablePoolManager;

import static org.junit.Assert.*;

public class ConfigUtilsTest {

    @Test
    public void testGetConfig() {
        Map<String, String> config = ConfigUtils.getConfig(HTablePoolManager.DEFAULT_INSTANCE);
        assertEquals("{hhbase.table.references.max=10, zookeeper.session.timeout=180000, " +
                "hbase.zookeeper.property.clientPort=2181, " +
                "hbase.zookeeper.quorum=10.20.16.32,10.20.16.35,10.20.16.36}", config.toString());
    }

}
