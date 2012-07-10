package org.phw.hbasedao.simple;

import java.util.List;

import org.phw.hbasedao.annotations.HBaseTable;
import org.phw.hbasedao.annotations.HColumn;
import org.phw.hbasedao.annotations.HRowkey;

/**
 * @author 张寅
 * @since 2012-3-27
 * @see
 */
@HBaseTable(name = "EopAppRequestBean", autoCreate = true)
public class AppHBean {

    @HRowkey
    private long rowkey;
    @HColumn
    private List<String> colcontent;

    public long getRowkey() {
        return rowkey;
    }

    public void setRowkey(long rowkey) {
        this.rowkey = rowkey;
    }

    public List<String> getColcontent() {
        return colcontent;
    }

    public void setColcontent(List<String> colcontent) {
        this.colcontent = colcontent;
    }
}
