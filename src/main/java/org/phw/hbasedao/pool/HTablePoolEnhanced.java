package org.phw.hbasedao.pool;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.Scan;

/**
 * 增强HTablePool.
 * @author BingooHuang
 *
 */
class HTablePoolEnhanced extends HTablePool {

    /**
     * Default Constructor.  Default HBaseConfiguration and no limit on pool size.
     */
    public HTablePoolEnhanced() {
        super();
    }

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
        return new PooledHTable(super.getTable(tableName));
    }

    /**
     * 池化的HTable.
     * @author BingooHuang
     *
     */
    public class PooledHTable implements HTableInterface {
        private HTableInterface table;

        /**
         * 构造函数。
         * @param table HTableInterface
         */
        public PooledHTable(HTableInterface table) {
            this.table = table;
        }

        @Override
        public void close() throws IOException {
            // 归还到池中。
            putTable(table);
        }

        @Override
        public byte[] getTableName() {
            return table.getTableName();
        }

        @Override
        public Configuration getConfiguration() {
            return table.getConfiguration();
        }

        @Override
        public HTableDescriptor getTableDescriptor() throws IOException {
            return table.getTableDescriptor();
        }

        @Override
        public boolean exists(Get get) throws IOException {
            return table.exists(get);
        }

        @Override
        public void batch(List<Row> actions, Object[] results) throws IOException, InterruptedException {
            table.batch(actions, results);
        }

        @Override
        public Object[] batch(List<Row> actions) throws IOException, InterruptedException {
            return table.batch(actions);
        }

        @Override
        public Result get(Get get) throws IOException {
            return table.get(get);
        }

        @Override
        public Result[] get(List<Get> gets) throws IOException {
            return table.get(gets);
        }

        @Override
        public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
            return table.getRowOrBefore(row, family);
        }

        @Override
        public ResultScanner getScanner(Scan scan) throws IOException {
            return table.getScanner(scan);
        }

        @Override
        public ResultScanner getScanner(byte[] family) throws IOException {
            return table.getScanner(family);
        }

        @Override
        public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
            return table.getScanner(family, qualifier);
        }

        @Override
        public void put(Put put) throws IOException {
            table.put(put);
        }

        @Override
        public void put(List<Put> puts) throws IOException {
            table.put(puts);
        }

        @Override
        public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put)
                throws IOException {
            return table.checkAndPut(row, family, qualifier, value, put);
        }

        @Override
        public void delete(Delete delete) throws IOException {
            table.delete(delete);
        }

        @Override
        public void delete(List<Delete> deletes) throws IOException {
            table.delete(deletes);
        }

        @Override
        public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete)
                throws IOException {
            return table.checkAndDelete(row, family, qualifier, value, delete);
        }

        @Override
        public Result increment(Increment increment) throws IOException {
            return table.increment(increment);
        }

        @Override
        public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount) throws IOException {
            return table.incrementColumnValue(row, family, qualifier, amount);
        }

        @Override
        public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, boolean writeToWAL)
                throws IOException {
            return table.incrementColumnValue(row, family, qualifier, amount, writeToWAL);
        }

        @Override
        public boolean isAutoFlush() {
            return table.isAutoFlush();
        }

        @Override
        public void flushCommits() throws IOException {
            table.flushCommits();

        }

        @Override
        public RowLock lockRow(byte[] row) throws IOException {
            return table.lockRow(row);
        }

        @Override
        public void unlockRow(RowLock rl) throws IOException {
            table.unlockRow(rl);
        }
    }
}
