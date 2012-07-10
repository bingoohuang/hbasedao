package org.phw.hbasedao.impl;

import java.util.Map;

import org.phw.hbasedao.annotations.HBaseTable;
import org.phw.hbasedao.annotations.HColumn;
import org.phw.hbasedao.annotations.HDynamic;
import org.phw.hbasedao.annotations.HRowkey;
import org.phw.hbasedao.annotations.HTypePair;

@HBaseTable(name = "ann_demo", autoCreate = true, families = { "f", "f1", "f2" })
public class AnnDemoBean {
    @HRowkey
    private String rowkey;
    @HColumn
    private int col1;
    @HColumn(family = "f1")
    private char col2;
    @HColumn(family = "f2")
    private byte[] col3;
    @HDynamic(mapping = { @HTypePair(keyType = String.class, valueType = String.class) }, family = "f")
    private Map<Object, Object> dyn;

    public void setRowkey(String rowkey) {
        this.rowkey = rowkey;
    }

    public String getRowkey() {
        return rowkey;
    }

    public void setCol1(int col1) {
        this.col1 = col1;
    }

    public int getCol1() {
        return col1;
    }

    public void setCol2(char col2) {
        this.col2 = col2;
    }

    public char getCol2() {
        return col2;
    }

    public void setCol3(byte[] col3) {
        this.col3 = col3;
    }

    public byte[] getCol3() {
        return col3;
    }

    public void setDyn(Map<Object, Object> dyn) {
        this.dyn = dyn;
    }

    public Map<Object, Object> getDyn() {
        return dyn;
    }
}
