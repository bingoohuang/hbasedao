package org.phw.hbasedao.cdr;

import org.phw.hbasedao.annotations.HBaseTable;
import org.phw.hbasedao.annotations.HColumn;
import org.phw.hbasedao.annotations.HRowkey;

@HBaseTable(name = "CdrBatch", autoCreate = true)
public class CdrBatch {
    @HRowkey
    private long timestamp;
    @HColumn
    private long start;
    @HColumn
    private long end;

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }
}
