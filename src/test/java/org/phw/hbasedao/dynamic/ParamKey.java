package org.phw.hbasedao.dynamic;

import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;
import org.phw.hbasedao.BytesConvertable;

public class ParamKey implements BytesConvertable<ParamKey> {
    private byte[] paramKey = Bytes.toBytes("paramkey");
    private long eff;
    private long exp;

    @Override
    public byte[] toBytes(ParamKey object) {
        return Bytes.add(paramKey, Bytes.toBytes(object.getEff()), Bytes.toBytes(object.getExp()));
    }

    @Override
    public ParamKey fromBytes(byte[] bytes) {
        if (bytes.length != paramKey.length + Bytes.SIZEOF_LONG + Bytes.SIZEOF_LONG) {
            return null;
        }

        if (!Bytes.equals(paramKey, Bytes.head(bytes, paramKey.length))) {
            return null;
        }

        this.eff = Bytes.toLong(bytes, paramKey.length, Bytes.SIZEOF_LONG);
        this.exp = Bytes.toLong(bytes, paramKey.length + Bytes.SIZEOF_LONG, Bytes.SIZEOF_LONG);
        return this;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (eff ^ eff >>> 32);
        result = prime * result + (int) (exp ^ exp >>> 32);
        result = prime * result + Arrays.hashCode(paramKey);
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
        ParamKey other = (ParamKey) obj;
        if (eff != other.eff) {
            return false;
        }
        if (exp != other.exp) {
            return false;
        }
        if (!Arrays.equals(paramKey, other.paramKey)) {
            return false;
        }
        return true;
    }

    public long getEff() {
        return eff;
    }

    public void setEff(long eff) {
        this.eff = eff;
    }

    public long getExp() {
        return exp;
    }

    public void setExp(long exp) {
        this.exp = exp;
    }

}
