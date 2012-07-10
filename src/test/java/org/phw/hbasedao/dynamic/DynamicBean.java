package org.phw.hbasedao.dynamic;

import java.util.Map;

import org.phw.hbasedao.annotations.HBaseTable;
import org.phw.hbasedao.annotations.HColumn;
import org.phw.hbasedao.annotations.HDynamic;
import org.phw.hbasedao.annotations.HRowkey;
import org.phw.hbasedao.annotations.HTypePair;

@HBaseTable(name = "dynamicbean", autoCreate = true, families = { "f" })
public class DynamicBean {
    @HRowkey
    private String rowkey;
    @HColumn
    private long appid;
    @HColumn
    private String desc;

    @HDynamic(mapping = {
            @HTypePair(keyType = SignKey.class, valueType = String.class),
            @HTypePair(keyType = ParamKey.class, valueType = ParamKeyValue.class),
            @HTypePair(keyType = String.class, valueType = String.class)
    })
    private Map<Object, Object> dynamicProperties /*= new HashMap<Object, Object>()*/;

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (appid ^ appid >>> 32);
        result = prime * result + (desc == null ? 0 : desc.hashCode());
        result = prime * result + (dynamicProperties == null ? 0 : dynamicProperties.hashCode());
        result = prime * result + (rowkey == null ? 0 : rowkey.hashCode());
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
        DynamicBean other = (DynamicBean) obj;
        if (appid != other.appid) {
            return false;
        }
        if (desc == null) {
            if (other.desc != null) {
                return false;
            }
        }
        else if (!desc.equals(other.desc)) {
            return false;
        }
        if (dynamicProperties == null) {
            if (other.dynamicProperties != null) {
                return false;
            }
        }
        else if (!dynamicProperties.equals(other.dynamicProperties)) {
            return false;
        }
        if (rowkey == null) {
            if (other.rowkey != null) {
                return false;
            }
        }
        else if (!rowkey.equals(other.rowkey)) {
            return false;
        }
        return true;
    }

    public String getRowkey() {
        return rowkey;
    }

    public void setRowkey(String rowkey) {
        this.rowkey = rowkey;
    }

    public long getAppid() {
        return appid;
    }

    public void setAppid(long appid) {
        this.appid = appid;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public Map<Object, Object> getDynamicProperties() {
        return dynamicProperties;
    }

    public void setDynamicProperties(Map<Object, Object> dynamicProperties) {
        this.dynamicProperties = dynamicProperties;
    }

}
