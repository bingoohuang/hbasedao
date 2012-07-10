package org.phw.hbasedao.complex2;

import org.apache.hadoop.hbase.util.Bytes;
import org.phw.hbasedao.BytesConvertable;

public class EopAppBeanRowkey implements BytesConvertable<EopAppBeanRowkey> {
    private String appcode;
    private long timestamp;

    @Override
    public EopAppBeanRowkey fromBytes(byte[] bytes) {
        if (bytes == null || bytes.length <= Bytes.SIZEOF_LONG) {
            return null;
        }

        int strBytesLen = bytes.length - Bytes.SIZEOF_LONG;
        this.setTimestamp(Bytes.toLong(bytes, strBytesLen));
        this.setAppcode(Bytes.toString(bytes, 0, strBytesLen));

        return this;
    }

    @Override
    public byte[] toBytes(EopAppBeanRowkey bean) {
        return Bytes.add(Bytes.toBytes(this.appcode), Bytes.toBytes(this.timestamp));
    }

    public String getAppcode() {
        return appcode;
    }

    public void setAppcode(String appcode) {
        this.appcode = appcode;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

}
