package org.phw.hbasedao.dynamic;

import org.apache.hadoop.hbase.util.Bytes;
import org.phw.hbasedao.BytesConvertable;

public class ParamKeyValue implements BytesConvertable<ParamKeyValue> {
    private long salt;
    private String security;

    @Override
    public byte[] toBytes(ParamKeyValue object) {
        return Bytes.add(Bytes.toBytes(object.getSalt()), Bytes.toBytes(object.getSecurity()));
    }

    @Override
    public ParamKeyValue fromBytes(byte[] bytes) {
        this.salt = Bytes.toLong(bytes, 0, Bytes.SIZEOF_LONG);
        this.security = Bytes.toString(bytes, Bytes.SIZEOF_LONG, bytes.length - Bytes.SIZEOF_LONG);
        return this;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (salt ^ salt >>> 32);
        result = prime * result + (security == null ? 0 : security.hashCode());
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
        ParamKeyValue other = (ParamKeyValue) obj;
        if (salt != other.salt) {
            return false;
        }
        if (security == null) {
            if (other.security != null) {
                return false;
            }
        }
        else if (!security.equals(other.security)) {
            return false;
        }
        return true;
    }

    public String getSecurity() {
        return security;
    }

    public void setSecurity(String security) {
        this.security = security;
    }

    public long getSalt() {
        return salt;
    }

    public void setSalt(long salt) {
        this.salt = salt;
    }

}
