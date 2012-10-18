package org.phw.hbasedao.env;

import org.junit.Test;
import org.phw.hbasedao.pool.HTablePoolManager;

public class HTablePoolManagerTest {
    @Test
    public void test() {
        HTablePoolManager.getHTablePool(HTablePoolManager.DEFAULT_INSTANCE, "127.0.0.1", "2181");
    }
}
