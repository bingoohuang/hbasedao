package org.phw.hbasedao;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.RowLock;

public class DaoRowLock {
    private HTableInterface hTable;
    private RowLock rowLock;

    public HTableInterface gethTable() {
        return hTable;
    }

    public void sethTable(HTableInterface hTable) {
        this.hTable = hTable;
    }

    public RowLock getRowLock() {
        return rowLock;
    }

    public void setRowLock(RowLock rowLock) {
        this.rowLock = rowLock;
    }
}
