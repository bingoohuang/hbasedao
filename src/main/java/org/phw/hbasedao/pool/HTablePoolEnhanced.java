package org.phw.hbasedao.pool;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

/**
 * 增强HTablePool.
 * @author BingooHuang
 *
 */
class HTablePoolEnhanced extends HTablePool {

    /**
     * Constructor to set maximum versions and use the specified configuration.
     * @param config configuration
     * @param maxSize maximum number of references to keep for each table
     */
    public HTablePoolEnhanced(final Configuration config, final int maxSize) {
        super(config, maxSize);
    }

    @Override
    public HTableInterface getTable(String tableName) {
        return (HTableInterface) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] { HTableInterface.class },
                new HTableProxy(super.getTable(tableName), this));
    }

    public static class HTableProxy implements InvocationHandler {
        private HTableInterface table;
        private HTablePool htablePool;

        public HTableProxy(HTableInterface table, HTablePool htablePool) {
            this.table = table;
            this.htablePool = htablePool;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (!method.getName().equals("close")) return method.invoke(table, args);

            htablePool.putTable(table);
            //            closeZkWatcher();
            return null;
        }

        //        private void closeZkWatcher() throws IOException {
        //            if (!(table instanceof HTable)) return;
        //
        //            ZooKeeperWatcher watcher = ((HTable) table).getConnection().getZooKeeperWatcher();
        //            if (watcher == null) return;
        //            watcher.close();
        //        }
    }

}
