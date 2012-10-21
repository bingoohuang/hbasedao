package org.phw.hbasedao.impl;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.phw.hbasedao.ex.HTableDefException;
import org.phw.hbasedao.pool.HTablePoolManager;

public class HBaseAdminMgr {

    public static HBaseAdmin createAdmin(String hbaseInstanceName) throws HTableDefException  {
        try {
            return new HBaseAdmin(HTablePoolManager.getHBaseConfig(hbaseInstanceName));
        } catch (MasterNotRunningException e) {
            throw new HTableDefException(e);
        } catch (ZooKeeperConnectionException e) {
            throw new HTableDefException(e);
        }
    }

    public static void close(HBaseAdmin admin) {
        if (admin == null) return;

        // HACK: HBasedmin copies Configuration which is used as connection cache key.
        // So we need to delete it after using or it will cause zk connection leak.
        // The second parameter stopProxy is not clear to me. I just set it to true.
        HConnectionManager.deleteConnection(admin.getConfiguration(), true);
    }


}
