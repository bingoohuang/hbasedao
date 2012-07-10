package org.phw.hbasedao.rowkeypart;

import org.phw.hbasedao.annotations.HBaseTable;
import org.phw.hbasedao.annotations.HColumn;
import org.phw.hbasedao.annotations.HRowkeyPart;

@HBaseTable(name = "partbean", autoCreate = true)
public class PartBean {
    @HRowkeyPart
    private String code;
    @HRowkeyPart
    private int id;
    @HColumn
    private String value;

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (code == null ? 0 : code.hashCode());
        result = prime * result + id;
        result = prime * result + (value == null ? 0 : value.hashCode());
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
        PartBean other = (PartBean) obj;
        if (code == null) {
            if (other.code != null) {
                return false;
            }
        }
        else if (!code.equals(other.code)) {
            return false;
        }
        if (id != other.id) {
            return false;
        }
        if (value == null) {
            if (other.value != null) {
                return false;
            }
        }
        else if (!value.equals(other.value)) {
            return false;
        }
        return true;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
