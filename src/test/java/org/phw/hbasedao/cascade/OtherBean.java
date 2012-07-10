package org.phw.hbasedao.cascade;

import org.phw.hbasedao.annotations.HBaseTable;
import org.phw.hbasedao.annotations.HColumn;
import org.phw.hbasedao.annotations.HRowkey;

@HBaseTable(name = "otherbean", autoCreate = true)
public class OtherBean {
    @HRowkey
    private String id;
    @HColumn
    private String color;

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (color == null ? 0 : color.hashCode());
        result = prime * result + (id == null ? 0 : id.hashCode());
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
        OtherBean other = (OtherBean) obj;
        if (color == null) {
            if (other.color != null) {
                return false;
            }
        }
        else if (!color.equals(other.color)) {
            return false;
        }
        if (id == null) {
            if (other.id != null) {
                return false;
            }
        }
        else if (!id.equals(other.id)) {
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

    public String getColor() {
        return color;
    }

    public void setColor(String color) {
        this.color = color;
    }

}
