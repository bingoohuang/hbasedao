package org.phw.hbasedao.util;

import org.apache.hadoop.hbase.util.Bytes;
import org.phw.hbasedao.BytesConvertable;

public class SampleBean2 implements BytesConvertable<SampleBean2> {
    private String name;
    private int age;

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getAge() {
        return age;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + age;
        result = prime * result + (name == null ? 0 : name.hashCode());
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
        SampleBean2 other = (SampleBean2) obj;
        if (age != other.age) {
            return false;
        }
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        }
        else if (!name.equals(other.name)) {
            return false;
        }
        return true;
    }

    @Override
    public SampleBean2 fromBytes(byte[] bytes) {
        if (bytes == null || bytes.length <= Bytes.SIZEOF_INT) {
            return null;
        }

        int strBytesLen = bytes.length - Bytes.SIZEOF_INT;
        SampleBean2 temp = new SampleBean2();
        temp.setAge(Bytes.toInt(bytes, strBytesLen));
        temp.setName(Bytes.toString(bytes, 0, strBytesLen));

        return temp;
    }

    @Override
    public byte[] toBytes(SampleBean2 object) {
        return Bytes.add(Bytes.toBytes(name),
                Bytes.toBytes(age));
    }

}
