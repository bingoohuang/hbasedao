package org.phw.hbasedao.cascade;

import org.phw.hbasedao.annotations.HBaseTable;
import org.phw.hbasedao.annotations.HColumn;
import org.phw.hbasedao.annotations.HRowkeyPart;

@HBaseTable(name = "subitem2", autoCreate = true)
public class SubItem2Bean {
    @HRowkeyPart
    private String id;
    @HRowkeyPart()
    private int seq;
    @HColumn
    private String itemName;

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (id == null ? 0 : id.hashCode());
        result = prime * result + (itemName == null ? 0 : itemName.hashCode());
        result = prime * result + seq;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        SubItem2Bean other = (SubItem2Bean) obj;
        if (id == null) {
            if (other.id != null) {
                return false;
            }
        }
        else if (!id.equals(other.id)) {
            return false;
        }
        if (itemName == null) {
            if (other.itemName != null) {
                return false;
            }
        }
        else if (!itemName.equals(other.itemName)) {
            return false;
        }
        if (seq != other.seq) {
            return false;
        }
        return true;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getItemName() {
        return itemName;
    }

    public void setItemName(String itemName) {
        this.itemName = itemName;
    }

    public int getSeq() {
        return seq;
    }

    public void setSeq(int seq) {
        this.seq = seq;
    }
}
