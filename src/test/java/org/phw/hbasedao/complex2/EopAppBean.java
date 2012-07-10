package org.phw.hbasedao.complex2;

import java.util.HashMap;
import java.util.Map;

import org.phw.hbasedao.annotations.HBaseTable;
import org.phw.hbasedao.annotations.HColumn;
import org.phw.hbasedao.annotations.HDynamic;
import org.phw.hbasedao.annotations.HRowkey;
import org.phw.hbasedao.annotations.HTypePair;

@HBaseTable(name = "eop_app", autoCreate = true, families = { "f" })
public class EopAppBean {
    @HRowkey
    private EopAppBeanRowkey rowkey;
    @HColumn
    private long appid;
    @HColumn
    private String desc;
    @HDynamic(mapping = {
            @HTypePair(keyType = String.class, valueType = String.class)
    })
    private Map<Object, Object> dynamicProperties = new HashMap<Object, Object>();

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

    public EopAppBeanRowkey getRowkey() {
        return rowkey;
    }

    public void setRowkey(EopAppBeanRowkey rowkey) {
        this.rowkey = rowkey;
    }
}
