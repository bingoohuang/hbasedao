package org.phw.hbasedao.dynamictablename;

import org.phw.hbasedao.annotations.HBaseTable;
import org.phw.hbasedao.annotations.HColumn;
import org.phw.hbasedao.annotations.HRowkey;
import org.phw.hbasedao.impl.ContextNameCreator;

@HBaseTable(name = "basename", nameCreator = ContextNameCreator.class, autoCreate = true)
public class DynamicTableNameBean {

    @HRowkey
    private long rowkey;
    @HColumn
    private String name;

    public long getRowkey() {
        return rowkey;
    }

    public void setRowkey(long rowkey) {
        this.rowkey = rowkey;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
