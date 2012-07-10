package org.phw.hbasedao.complex1;

import org.apache.hadoop.hbase.util.Bytes;
import org.phw.hbasedao.BytesConvertable;

public class OtherDay implements BytesConvertable<OtherDay> {
    private int year;
    private int month;
    private int day;

    @Override
    public byte[] toBytes(OtherDay object) {
        return Bytes.add(Bytes.toBytes(year), Bytes.toBytes(month), Bytes.toBytes(day));
    }

    @Override
    public OtherDay fromBytes(byte[] bytes) {
        if (bytes == null || bytes.length != Bytes.SIZEOF_INT * 3) {
            return null;
        }

        year = Bytes.toInt(bytes, 0, Bytes.SIZEOF_INT);
        month = Bytes.toInt(bytes, Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
        day = Bytes.toInt(bytes, Bytes.SIZEOF_INT * 2, Bytes.SIZEOF_INT);
        return this;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

}
