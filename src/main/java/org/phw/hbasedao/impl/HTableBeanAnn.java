package org.phw.hbasedao.impl;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;
import org.phw.hbasedao.annotations.HBaseTable;
import org.phw.hbasedao.annotations.HColumn;
import org.phw.hbasedao.annotations.HDynamic;
import org.phw.hbasedao.annotations.HRowkeyPart;
import org.phw.hbasedao.ex.HDaoException;
import org.phw.hbasedao.ex.HTableDefException;
import org.phw.hbasedao.util.Fields;
import org.phw.hbasedao.util.Strs;
import org.phw.hbasedao.util.Types;

import com.esotericsoftware.reflectasm.FieldAccess;
import com.esotericsoftware.reflectasm.MethodAccess;

import static com.google.common.collect.Lists.*;

import static com.google.common.collect.Sets.*;

public class HTableBeanAnn {
    private Class<?> beanClass;

    private Field hRowkeyField;
    private Field hParentField;

    private MethodAccess methodAccess;
    private FieldAccess fieldAccess;

    private ArrayList<Field> hRelateToFields = newArrayList();
    private ArrayList<Field> hRowkeyPartFields = newArrayList();
    private int knownRowkeyPartsBytesLen = 0;
    private int unkownPartsBytesLen = 0;
    private ArrayList<Field> hColumnFields = newArrayList();
    private ArrayList<Field> hDynamicFields = newArrayList();

    private Set<String> families = newHashSet();
    private Set<byte[]> bfamilies = newHashSet();

    public int getKnownRowkeyPartsBytesLen() {
        return knownRowkeyPartsBytesLen;
    }

    public int getUnkownRowkeyPartsBytesLen() {
        return unkownPartsBytesLen;
    }

    public void afterPropertiesSet() throws HTableDefException {
        for (String f : families) {
            bfamilies.add(Bytes.toBytes(f));
        }

        knownRowkeyPartsBytesLen = 0;
        unkownPartsBytesLen = 0;
        for (Field hRowkeyPartField : hRowkeyPartFields) {
            HRowkeyPart hRowkeyPart = hRowkeyPartField.getAnnotation(HRowkeyPart.class);
            int bytesLen = hRowkeyPart.bytesLen();
            if (bytesLen <= 0) {
                bytesLen = Types.getBytesLen(hRowkeyPartField.getType());
            }
            if (bytesLen <= 0) {
                ++unkownPartsBytesLen;
            }
            else {
                knownRowkeyPartsBytesLen += bytesLen;
            }
        }

        if (unkownPartsBytesLen > 1) {
            throw new HTableDefException("More than one @RowkeyPart has not defined fixed bytesLen");
        }
    }

    public byte[] getRowkey(Object bean) {
        if (bean instanceof byte[]) {
            return (byte[]) bean;
        }

        if (hRowkeyField != null) {
            Object rowkeyValue = bean;
            if (bean.getClass() == beanClass) {
                rowkeyValue = Fields
                        .getFieldValue(methodAccess, fieldAccess, bean, getHRowkeyField());
            }

            return Types.toBytes(rowkeyValue);
        }

        byte[] rowkeyBytes = null;
        for (Field hRowkeyPartField : hRowkeyPartFields) {
            Object rowkeyPartValue = Fields.getFieldValue(methodAccess, fieldAccess, bean, hRowkeyPartField);
            rowkeyBytes = rowkeyBytes == null ? Types.toBytes(rowkeyPartValue)
                    : Bytes.add(rowkeyBytes, Types.toBytes(rowkeyPartValue));
        }

        return rowkeyBytes;
    }

    public void setRowkey(Object bean, byte[] bRowkey) throws HDaoException {
        if (hRowkeyField != null) {
            Fields.setFieldValue(methodAccess, fieldAccess, bean, hRowkeyField,
                    Types.fromBytes(bRowkey, hRowkeyField.getType()));
            return;
        }

        int offset = 0;
        for (Field hRowkeyPartField : hRowkeyPartFields) {
            HRowkeyPart hRowkeyPart = hRowkeyPartField.getAnnotation(HRowkeyPart.class);
            int bytesLen = hRowkeyPart.bytesLen();
            if (bytesLen <= 0) {
                bytesLen = Types.getBytesLen(hRowkeyPartField.getType());
            }
            if (bytesLen <= 0) {
                bytesLen = bRowkey.length - knownRowkeyPartsBytesLen;
            }
            if (bytesLen <= 0) {
                throw new HDaoException("rowkey bytes cannot converted to @RowkeyPart fields' value");
            }

            byte[] rowkeyPartBytes = new byte[bytesLen];
            System.arraycopy(bRowkey, offset, rowkeyPartBytes, 0, bytesLen);
            offset += bytesLen;
            Object rowkeyPartValue = Types.fromBytes(rowkeyPartBytes, hRowkeyPartField.getType());
            Fields.setFieldValue(methodAccess, fieldAccess, bean, hRowkeyPartField, rowkeyPartValue);
        }
    }

    public void setHBaseTable(Class<?> beanClass) {
        this.setBeanClass(beanClass);
        HBaseTable hbaseTable = beanClass.getAnnotation(HBaseTable.class);
        if (hbaseTable.families() != null) {
            for (String family : hbaseTable.families()) {
                families.add(family);
            }
        }
    }

    public void setHRowkey(Field field) {
        this.hRowkeyField = field;
    }

    public void addHColumn(Field field) {
        hColumnFields.add(field);
        HColumn hcolumn = field.getAnnotation(HColumn.class);
        if (Strs.isNotEmpty(hcolumn.family())) {
            families.add(hcolumn.family());
        }
    }

    public void addHRowkeyPart(Field field) {
        hRowkeyPartFields.add(field);
    }

    public void addHRelateTo(Field field) {
        hRelateToFields.add(field);
    }

    public Field getHRowkeyField() {
        return hRowkeyField;
    }

    public void setRowkeyField(Field rowkeyField) {
        this.hRowkeyField = rowkeyField;
    }

    public void addHDynamic(Field field) {
        this.hDynamicFields.add(field);
        HDynamic hdynamic = field.getAnnotation(HDynamic.class);
        if (Strs.isNotEmpty(hdynamic.family())) {
            families.add(hdynamic.family());
        }
    }

    public Class<?> getBeanClass() {
        return beanClass;
    }

    public void setBeanClass(Class<?> beanClass) {
        this.beanClass = beanClass;
        this.methodAccess = MethodAccess.get(beanClass);
        this.fieldAccess = FieldAccess.get(beanClass);
    }

    public ArrayList<Field> getHColumnFields() {
        return hColumnFields;
    }

    public ArrayList<Field> getHDynamicFields() {
        return hDynamicFields;
    }

    public Set<String> getFamilies() {
        return families;
    }

    public void setFamilies(Set<String> families) {
        this.families = families;
    }

    public Set<byte[]> getBfamilies() {
        return bfamilies;
    }

    public void setBfamilies(Set<byte[]> bfamilies) {
        this.bfamilies = bfamilies;
    }

    public ArrayList<Field> getHRowkeyPartFields() {
        return hRowkeyPartFields;
    }

    public ArrayList<Field> getHRelateToFields() {
        return hRelateToFields;
    }

    public void setHRelateToFields(ArrayList<Field> hrelateFields) {
        this.hRelateToFields = hrelateFields;
    }

    public Field getHParentField() {
        return hParentField;
    }

    public void setHParent(Field field) {
        hParentField = field;
    }

    public HBaseTable getHBaseTable() {
        return beanClass.getAnnotation(HBaseTable.class);
    }

    public MethodAccess getMethodAccess() {
        return methodAccess;
    }

    public FieldAccess getFieldAccess() {
        return fieldAccess;
    }

}
