package org.phw.hbasedao;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.phw.hbasedao.annotations.HBaseTable;
import org.phw.hbasedao.annotations.HCascade;
import org.phw.hbasedao.annotations.HColumn;
import org.phw.hbasedao.annotations.HDynamic;
import org.phw.hbasedao.annotations.HTypePair;
import org.phw.hbasedao.ex.EmptyValueException;
import org.phw.hbasedao.ex.FamilyEmptyException;
import org.phw.hbasedao.ex.HDaoException;
import org.phw.hbasedao.ex.HTableDefException;
import org.phw.hbasedao.impl.HTableBeanAnn;
import org.phw.hbasedao.impl.HTableBeanAnnMgr;
import org.phw.hbasedao.pool.HTablePoolManager;
import org.phw.hbasedao.util.Clazz;
import org.phw.hbasedao.util.Fields;
import org.phw.hbasedao.util.Hex;
import org.phw.hbasedao.util.Strs;
import org.phw.hbasedao.util.Types;

public class DefaultHDao extends BaseHDao {
    private static final int FETCH_ROWS = 1000;
    private String hbaesInstanceName;

    public DefaultHDao() {
        this(HTablePoolManager.DEFAULT_INSTANCE);
    }

    public DefaultHDao(String hbaesInstanceName) {
        this.hbaesInstanceName = hbaesInstanceName;
    }

    @Override
    public <T> void delete(String family, Class<T> beanClass, Object rowkey, Object key, Object... keys)
            throws HDaoException {
        HTableBeanAnn ann = HTableBeanAnnMgr.getBeanAnn(hbaesInstanceName, beanClass);
        byte[] bRowkey = ann.getRowkey(rowkey);
        Delete delete = new Delete(bRowkey);
        createDeleteKeys(getDefaultFamily(ann, family), delete, key, keys);
        commitDelete(ann.getHBaseTable(), delete);
    }

    private void createDeleteKeys(String family, Delete delete, Object key, Object... keys) {
        byte[] famBytes = Bytes.toBytes(family);
        delete.deleteColumn(famBytes, Types.toBytes(key));
        for (Object key1 : keys)
            delete.deleteColumn(famBytes, Types.toBytes(key1));
    }

    @Override
    protected <T> boolean merge(T bean, boolean isInsert, EnumSet<DaoOption> options) throws HDaoException {
        HTableBeanAnn ann = HTableBeanAnnMgr.getBeanAnn(hbaesInstanceName, bean.getClass());
        byte[] bRowkey = ann.getRowkey(bean);
        DaoRowLock daoRowlock = null;
        try {
            daoRowlock = getRowLock(ann, bRowkey);

            Get get = new Get(bRowkey, daoRowlock.getRowLock());
            addKeyAndFirstKeyOnlyFilter(get);

            Result rs = getValues(ann, get);
            if (rs.isEmpty() ^ isInsert) { return false; }

            Put put = new Put(bRowkey, daoRowlock.getRowLock());
            createPutValues(bean, ann, put, options);
            commitPut(ann, put);
            return true;
        }
        finally {
            unlockRow(daoRowlock);
        }
    }

    private void addKeyAndFirstKeyOnlyFilter(Get get) {
        FilterList filterList = new FilterList();
        filterList.addFilter(new KeyOnlyFilter());
        filterList.addFilter(new FirstKeyOnlyFilter());
        get.setFilter(filterList);
    }

    @Override
    public <T> void put(T bean, EnumSet<DaoOption> options) throws HDaoException {
        HTableBeanAnn ann = HTableBeanAnnMgr.getBeanAnn(hbaesInstanceName, bean.getClass());
        byte[] bRowkey = ann.getRowkey(bean);

        Put put = new Put(bRowkey);
        createPutValues(bean, ann, put, options);
        commitPut(ann, put);
    }

    @Override
    public <T> void put(List<T> beans, EnumSet<DaoOption> options) throws HDaoException {
        if (beans == null || beans.size() == 0) { return; }

        HTableBeanAnn ann = HTableBeanAnnMgr.getBeanAnn(hbaesInstanceName, beans.get(0).getClass());
        ArrayList<Put> putsList = new ArrayList<Put>(beans.size());
        for (T bean : beans) {
            byte[] bRowkey = ann.getRowkey(bean);

            Put put = new Put(bRowkey);
            createPutValues(bean, ann, put, options);
            putsList.add(put);
        }

        commitPut(ann, putsList);
    }

    @Override
    protected <T> T getImpl(Class<T> beanClass, EnumSet<DaoOption> options, Object bean) throws HDaoException {
        HTableBeanAnn ann = HTableBeanAnnMgr.getBeanAnn(hbaesInstanceName, beanClass);
        byte[] bRowkey = ann.getRowkey(bean);
        return getImpl(bRowkey, beanClass, options);
    }

    private <T> T getImpl(byte[] bRowkey, Class<T> beanClass, EnumSet<DaoOption> options) throws HDaoException {
        HTableBeanAnn ann = HTableBeanAnnMgr.getBeanAnn(hbaesInstanceName, beanClass);
        Get get = new Get(bRowkey);
        Result rs = getValues(ann, get);
        if (rs.isEmpty()) return null;

        T retBean = Clazz.newInstance(beanClass);
        ann.setRowkey(retBean, bRowkey);
        setValues(bRowkey, retBean, ann, rs, options);

        return retBean;
    }

    @Override
    protected <T> T getImpl(Class<T> beanClass, Object bean, String family, String... families) throws HDaoException {
        HTableBeanAnn ann = HTableBeanAnnMgr.getBeanAnn(hbaesInstanceName, beanClass);
        byte[] bRowkey = ann.getRowkey(bean);

        Get get = new Get(bRowkey);
        Result rs = getValues(ann, get, family, families);
        if (rs.isEmpty())  return null; 

        T retBean = Clazz.newInstance(beanClass);
        ann.setRowkey(retBean, bRowkey);

        setValues(retBean, ann, rs, family, families);
        return retBean;
    }

    @Override
    public <T> T get(Class<T> beanClass, Object rowkey, String family, String... families) throws HDaoException {
        return getImpl(beanClass, rowkey, family, families);
    }

    @Override
    public <T> void delete(EnumSet<DaoOption> options, Class<T> beanClass, Object rowkey) throws HDaoException {
        HTableBeanAnn ann = HTableBeanAnnMgr.getBeanAnn(hbaesInstanceName, beanClass);
        byte[] bRowkey = ann.getRowkey(rowkey);
        delete(bRowkey, options, beanClass);
    }

    private <T> void delete(byte[] bRowkey, EnumSet<DaoOption> options, Class<T> beanClass) throws HDaoException {
        HTableBeanAnn ann = HTableBeanAnnMgr.getBeanAnn(hbaesInstanceName, beanClass);
        Delete delete = new Delete(bRowkey);
        commitDelete(ann.getHBaseTable(), delete);

        if (options.contains(DaoOption.CASCADE)) {
            for (Field field : ann.getHRelateToFields()) {
                HCascade hRelateTo = field.getAnnotation(HCascade.class);
                if (List.class.isAssignableFrom(field.getType())) {
                    cascadeDeleteByQuery(bRowkey, options, field, hRelateTo);
                }
                else if (HTableBeanAnnMgr.getBeanAnn(hbaesInstanceName, field.getType()) != null) {
                    delete(options, field.getType(), bRowkey);
                }
            }
        }
    }

    private void cascadeDeleteByQuery(byte[] bRowkey, EnumSet<DaoOption> options, Field field, HCascade hRelateTo)
            throws HTableDefException, HDaoException {
        Class<?> clazz = HTableBeanAnnMgr.getCascadeClass(field);

        HTableBeanAnn hRelateToAnn = HTableBeanAnnMgr.getBeanAnn(hbaesInstanceName, clazz);

        int rightBytesLen = hRelateTo.rowkeyBytesLen() > 0
                ? hRelateTo.rowkeyBytesLen() - bRowkey.length
                : hRelateToAnn.getUnkownRowkeyPartsBytesLen() <= 0
                        ? hRelateToAnn.getKnownRowkeyPartsBytesLen() - bRowkey.length
                        : hRelateToAnn.getKnownRowkeyPartsBytesLen();
        byte[] padding = new byte[rightBytesLen];
        for (int i = 0; i < rightBytesLen; i++) {
            padding[i] = 0;
        }
        byte[] startRow = Bytes.add(bRowkey, padding);
        for (int i = 0; i < rightBytesLen; i++) {
            padding[i] = (byte) 0xFF;
        }
        byte[] stopRow = Bytes.add(bRowkey, padding);

        List<?> lstRelatedValue = queryImpl(clazz, startRow, stopRow, 0, options);
        for (Object object : lstRelatedValue) {
            delete(options, clazz, hRelateToAnn.getRowkey(object));
        }
    }

    private Result getValues(HTableBeanAnn ann, Get get, String family, String... families) throws HDaoException {
        HTableInterface hTable = null;
        try {
            get.addFamily(Bytes.toBytes(family));
            for (String fam : families) {
                get.addFamily(Bytes.toBytes(fam));
            }

            hTable = getHTable(ann);
            return hTable.get(get);
        }
        catch (IOException e) {
            throw new HDaoException(e);
        }
        finally {
            closeHTable(hTable);
        }
    }

    private Result getValues(HTableBeanAnn ann, Get get) throws HDaoException {
        HTableInterface hTable = null;
        try {
            for (byte[] family : ann.getBfamilies()) {
                get.addFamily(family);
            }

            hTable = getHTable(ann);
            return hTable.get(get);
        }
        catch (IOException e) {
            throw new HDaoException(e);
        }
        finally {
            closeHTable(hTable);
        }
    }

    private HTableInterface getHTable(HTableBeanAnn ann) {
        return getHTable(ann.getHBaseTable());
    }

    private HTableInterface getHTable(HBaseTable hBaseTable) {
        return HTablePoolManager.getHTable(getTableName(hBaseTable), hbaesInstanceName);
    }

    private String getTableName(HBaseTable hBaseTable) {
        try {
            return HTableBeanAnnMgr.getTableName(hBaseTable);
        }
        catch (HTableDefException e) {
            // here should not happen b'coz there was table existance check before.s
            throw new RuntimeException(e);
        }
    }

    private void createPutValues(Object bean, HTableBeanAnn ann, Put put, EnumSet<DaoOption> options)
            throws HDaoException {
        int kvNums = 0;
        for (Field field : ann.getHColumnFields()) {
            kvNums += processHColumnFields(bean, ann, put, field);
        }
        for (Field field : ann.getHDynamicFields()) {
            kvNums += processHDynamicFields(bean, ann, put, field);
        }

        if (options.contains(DaoOption.CASCADE)) {
            for (Field field : ann.getHRelateToFields()) {
                processHRelateToFields(ann, bean, field);
            }
        }

        if (kvNums == 0) { throw new EmptyValueException("There is no values to do in this operation"); }
    }

    private void processHRelateToFields(HTableBeanAnn ann, Object bean, Field field) throws HDaoException {
        Object relateToValue = Fields.getFieldValue(ann.getMethodAccess(), ann.getFieldAccess(), bean, field);
        if (relateToValue == null) { return; }

        if (List.class.isAssignableFrom(field.getType())) {
            List<?> list = (List<?>) relateToValue;
            for (Object item : list) {
                put(item);
            }
        }
        else {
            put(relateToValue);
        }
    }

    private int processHDynamicFields(Object bean, HTableBeanAnn ann, Put put, Field field) throws HDaoException {
        Map<Object, Object> value = Fields.getFieldValue(ann.getMethodAccess(), ann.getFieldAccess(), bean, field);
        if (value == null || value.isEmpty()) return 0;

        HDynamic hdynamic = field.getAnnotation(HDynamic.class);
        String family = getDefaultFamily(ann, hdynamic.family());
        for (Entry<Object, Object> entry : value.entrySet()) {
            put.add(Bytes.toBytes(family), Types.toBytes(entry.getKey()), Types.toBytes(entry.getValue()));
        }

        return value.size();
    }

    private int processHColumnFields(Object bean, HTableBeanAnn ann, Put put, Field field)
            throws FamilyEmptyException {
        HColumn kvAnn = field.getAnnotation(HColumn.class);

        String keyValue = kvAnn.key();
        String key = keyValue == null || keyValue.length() == 0 ? field.getName() : keyValue;
        Object value = Fields.getFieldValue(ann.getMethodAccess(), ann.getFieldAccess(), bean, field);
        if (value == null) { return 0; }

        String family = getDefaultFamily(ann, kvAnn.family());
        put.add(Bytes.toBytes(family), Bytes.toBytes(key), Types.toBytes(value));
        return 1;
    }

    @Override
    public <T> DaoRowLock lockRow(Class<T> beanClass, Object rowkey) throws HDaoException {
        HTableBeanAnn ann = HTableBeanAnnMgr.getBeanAnn(hbaesInstanceName, beanClass);
        byte[] bRowkey = ann.getRowkey(rowkey);
        return getRowLock(ann, bRowkey);
    }

    private DaoRowLock getRowLock(HTableBeanAnn ann, byte[] bRowkey) throws HDaoException {
        HTableInterface hTable = null;
        try {
            hTable = getHTable(ann);
            RowLock rowLock = hTable.lockRow(bRowkey);
            DaoRowLock daoRowLock = new DaoRowLock();
            daoRowLock.sethTable(hTable);
            daoRowLock.setRowLock(rowLock);
            return daoRowLock;
        }
        catch (IOException e) {
            throw new HDaoException(e);
        }
    }

    @Override
    public void unlockRow(DaoRowLock daoRowlock) throws HDaoException {
        unlockRow(daoRowlock, true);
    }

    private void unlockRow(DaoRowLock daoRowlock, boolean closeHTable) throws HDaoException {
        if (daoRowlock == null) { return; }

        HTableInterface hTable = daoRowlock.gethTable();

        try {
            hTable.unlockRow(daoRowlock.getRowLock());
        }
        catch (IOException e) {
            throw new HDaoException(e);
        }
        finally {
            closeHTable(closeHTable ? hTable : null);
        }
    }

    private boolean isAllowedFamily(String strFamily, String family, String... families) {
        if (Strs.equals(strFamily, family)) { return true; }
        for (String fam : families) {
            if (Strs.equals(strFamily, fam)) { return true; }
        }
        return false;
    }

    private void setValues(Object bean, HTableBeanAnn ann, Result rs, String family, String... families)
            throws FamilyEmptyException {
        for (byte[] bfamily : ann.getBfamilies()) {
            String strFamily = Bytes.toString(bfamily);
            if (isAllowedFamily(strFamily, family, families)) {
                ArrayList<String> usedQualifiers = new ArrayList<String>();
                populateHColumn(bean, ann, rs, bfamily, strFamily, usedQualifiers);
                populateHDynamic(bean, ann, rs, bfamily, strFamily, usedQualifiers);
            }
        }
    }

    private void setValues(byte[] bRowkey, Object bean, HTableBeanAnn ann, Result rs, EnumSet<DaoOption> options)
            throws HDaoException {
        for (byte[] bfamily : ann.getBfamilies()) {
            String strFamily = Bytes.toString(bfamily);
            ArrayList<String> usedQualifiers = new ArrayList<String>();
            populateHColumn(bean, ann, rs, bfamily, strFamily, usedQualifiers);
            populateHDynamic(bean, ann, rs, bfamily, strFamily, usedQualifiers);
        }

        if (options.contains(DaoOption.CASCADE)) {
            for (Field field : ann.getHRelateToFields()) {
                HCascade hRelateTo = field.getAnnotation(HCascade.class);
                if (List.class.isAssignableFrom(field.getType())) {
                    processListRelateTo(ann, hRelateTo, bRowkey, bean, options, field);
                }
                else {
                    HTableBeanAnn beanAnn = HTableBeanAnnMgr.getBeanAnn(hbaesInstanceName, field.getType());
                    if (beanAnn != null) {
                        Object relateToValue = getImpl(bRowkey, field.getType(), options);
                        Fields.setFieldValue(ann.getMethodAccess(), ann.getFieldAccess(), bean, field, relateToValue);
                        HTableBeanAnn hRelateToAnn = HTableBeanAnnMgr.getBeanAnn(hbaesInstanceName,
                                field.getType());
                        if (hRelateToAnn.getHParentField() != null) {
                            Fields.setFieldValue(hRelateToAnn.getMethodAccess(), hRelateToAnn.getFieldAccess(),
                                    relateToValue, hRelateToAnn.getHParentField(), bean);
                        }
                    }
                }
            }
        }
    }

    private void processListRelateTo(HTableBeanAnn ann, HCascade hRelateTo, byte[] bRowkey, Object bean,
            EnumSet<DaoOption> options,
            Field field) throws HTableDefException, HDaoException {
        Class<?> clazz = HTableBeanAnnMgr.getCascadeClass(field);
        HTableBeanAnn hRelateToAnn = HTableBeanAnnMgr.getBeanAnn(hbaesInstanceName, clazz);

        int rightBytesLen = hRelateTo.rowkeyBytesLen() > 0
                ? hRelateTo.rowkeyBytesLen() - bRowkey.length
                : hRelateToAnn.getUnkownRowkeyPartsBytesLen() <= 0
                        ? hRelateToAnn.getKnownRowkeyPartsBytesLen() - bRowkey.length
                        : hRelateToAnn.getKnownRowkeyPartsBytesLen();
        byte[] padding = new byte[rightBytesLen];
        for (int i = 0; i < rightBytesLen; i++) {
            padding[i] = 0;
        }
        byte[] startRow = Bytes.add(bRowkey, padding);
        for (int i = 0; i < rightBytesLen; i++) {
            padding[i] = (byte) 0xFF;
        }
        byte[] stopRow = Bytes.add(bRowkey, padding);

        List<?> lstRelatedValue = queryImpl(clazz, startRow, stopRow, 0, options);
        if (hRelateToAnn.getHParentField() != null) {
            for (Object object : lstRelatedValue) {
                Fields.setFieldValue(hRelateToAnn.getMethodAccess(), hRelateToAnn.getFieldAccess(),
                        object, hRelateToAnn.getHParentField(), bean);
            }
        }

        List<Object> list = Fields.getFieldValue(ann.getMethodAccess(), ann.getFieldAccess(), bean, field);
        if (list == null) {
            Fields.setFieldValue(ann.getMethodAccess(), ann.getFieldAccess(), bean, field, lstRelatedValue);
        }
        else {
            for (Object object : lstRelatedValue) {
                list.add(object);
            }
        }
    }

    private void populateHDynamic(Object bean, HTableBeanAnn ann, Result rs, byte[] bfamily, String strFamily,
            ArrayList<String> usedQualifiers) throws FamilyEmptyException {
        NavigableMap<byte[], byte[]> familyMap = rs.getFamilyMap(bfamily);
        FAMILYMA: for (Entry<byte[], byte[]> entry : familyMap.entrySet()) {
            if (usedQualifiers.contains(Hex.toHex(entry.getKey()))) {
                continue;
            }

            for (Field field : ann.getHDynamicFields()) {
                HDynamic hdynamic = field.getAnnotation(HDynamic.class);
                String family = getDefaultFamily(ann, hdynamic.family());
                if (family.equals(strFamily)) {
                    HTypePair[] mapping = hdynamic.mapping();
                    for (HTypePair hTypePair : mapping) {
                        Object key = Types.fromBytes(entry.getKey(), hTypePair.keyType());
                        if (key != null) {
                            Object value = Types.fromBytes(entry.getValue(), hTypePair.valueType());
                            Map<Object, Object> map = Fields.getFieldValue(ann.getMethodAccess(), ann.getFieldAccess(),
                                    bean, field);
                            if (map == null) {
                                map = new HashMap<Object, Object>();
                                Fields.setFieldValue(ann.getMethodAccess(), ann.getFieldAccess(), bean, field, map);
                            }
                            map.put(key, value);
                            continue FAMILYMA;
                        }
                    }
                }
            }

        }
    }

    private void populateHColumn(Object bean, HTableBeanAnn ann, Result rs, byte[] bfamily, String strFamily,
            ArrayList<String> usedQualifiers) throws FamilyEmptyException {
        for (Field field : ann.getHColumnFields()) {
            HColumn hcolumn = field.getAnnotation(HColumn.class);
            String family = getDefaultFamily(ann, hcolumn.family());
            if (family.equals(strFamily)) {
                String key = Strs.defaultString(hcolumn.key(), field.getName());
                byte[] qualifier = Bytes.toBytes(key);
                usedQualifiers.add(Hex.toHex(qualifier));
                byte[] value = rs.getValue(bfamily, qualifier);
                Fields.setFieldValue(ann.getMethodAccess(), ann.getFieldAccess(), bean, field,
                        Types.fromBytes(value, field.getType()));
            }
        }
    }

    private String getDefaultFamily(HTableBeanAnn ann, String family) throws FamilyEmptyException {
        String retFamily = family;
        if (Strs.isEmpty(retFamily)) {
            String[] families = ann.getHBaseTable().families();
            retFamily = families != null && families.length > 0 ? families[0] : null;
        }
        if (Strs.isEmpty(retFamily)) { throw new FamilyEmptyException(); }
        return retFamily;
    }

    private void commitPut(HTableBeanAnn ann, Put put) throws HDaoException {
        HTableInterface hTable = null;
        try {
            hTable = getHTable(ann);
            hTable.put(put);
            hTable.flushCommits();
        }
        catch (IOException e) {
            throw new HDaoException(e);
        }
        finally {
            closeHTable(hTable);
        }
    }

    private void commitPut(HTableBeanAnn ann, List<Put> put) throws HDaoException {
        HTableInterface hTable = null;
        try {
            hTable = getHTable(ann);
            hTable.put(put);
            hTable.flushCommits();
        }
        catch (IOException e) {
            throw new HDaoException(e);
        }
        finally {
            closeHTable(hTable);
        }
    }

    private void commitDelete(HBaseTable htableAnn, Delete delete) throws HDaoException {
        HTableInterface hTable = null;
        try {
            hTable = getHTable(htableAnn);
            commitDelete(hTable, delete);
        }
        finally {
            closeHTable(hTable);
        }
    }

    private void commitDelete(HTableInterface hTable, Delete delete) throws HDaoException {
        try {
            hTable.delete(delete);
            hTable.flushCommits();
        }
        catch (IOException e) {
            throw new HDaoException(e);
        }
        finally {
            closeHTable(hTable);
        }
    }

    private void closeHTable(HTableInterface hTable) {
        if (hTable == null) return;
        try {
            hTable.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public <T, V> V get(String family, Class<T> beanClass, Object rowkey, Class<V> valueType, Object key)
            throws HDaoException {
        HTableBeanAnn ann = HTableBeanAnnMgr.getBeanAnn(hbaesInstanceName, beanClass);
        byte[] bRowkey = ann.getRowkey(rowkey);

        Get get = new Get(bRowkey);
        byte[] bkey = Types.toBytes(key);
        byte[] bfamily = Bytes.toBytes(getDefaultFamily(ann, family));
        Result rs = getSingleValue(ann, get, bfamily, bkey);
        if (rs.isEmpty()) { return null; }

        return Types.fromBytes(rs.getValue(bfamily, bkey), valueType);
    }

    private Result getSingleValue(HTableBeanAnn ann, Get get, byte[] family, byte[] key) throws HDaoException {
        HTableInterface hTable = null;
        try {
            get.addFamily(family);
            get.setFilter(new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(key)));
            hTable = getHTable(ann);
            return hTable.get(get);
        }
        catch (IOException e) {
            throw new HDaoException(e);
        }
        finally {
            closeHTable(hTable);
        }
    }

    @Override
    public <T> void put(String family, Class<T> beanClass, Object rowkey, Object key, Object value, Object... kvs)
            throws HDaoException {
        HTableBeanAnn ann = HTableBeanAnnMgr.getBeanAnn(hbaesInstanceName, beanClass);
        byte[] bRowkey = ann.getRowkey(rowkey);
        byte[] bfamily = Bytes.toBytes(getDefaultFamily(ann, family));
        Put put = new Put(bRowkey);
        createPutKv(ann, put, bfamily, key, value, kvs);
        commitPut(ann, put);
    }

    private <T> void createPutKv(HTableBeanAnn ann, Put put, byte[] family, Object key, Object value,
            Object[] kvs) {
        put.add(family, Types.toBytes(key), Types.toBytes(value));
        for (int i = 0; i < kvs.length; i += 2) {
            if (i + 1 < kvs.length) {
                put.add(family, Types.toBytes(kvs[i]), Types.toBytes(kvs[i + 1]));
            }
        }
    }

    @Override
    protected <T> List<T> queryImpl(Class<T> beanClass, Object startRowkey, Object stopRowkey, int maxRows,
            EnumSet<DaoOption> options) throws HDaoException {
        HTableBeanAnn ann = HTableBeanAnnMgr.getBeanAnn(hbaesInstanceName, beanClass);
        byte[] startRow = ann.getRowkey(startRowkey);
        byte[] stopRow = stopRowkey != null ? ann.getRowkey(stopRowkey) : null;
        return queryImpl(beanClass, startRow, stopRow, maxRows, options);
    }

    private <T> List<T> queryImpl(Class<T> beanClass, byte[] startRow, byte[] stopRow, int maxRows,
            EnumSet<DaoOption> options)
            throws HDaoException {
        HTableBeanAnn ann = HTableBeanAnnMgr.getBeanAnn(hbaesInstanceName, beanClass);
        Scan scan = stopRow != null ? new Scan(startRow, stopRow) : new Scan(startRow);
        HTableInterface hTable = null;
        try {
            for (byte[] family : ann.getBfamilies()) {
                scan.addFamily(family);
            }

            hTable = getHTable(ann);
            ResultScanner rr = hTable.getScanner(scan);

            int rows = 0;
            ArrayList<T> arrayList = new ArrayList<T>();
            L: for (Result[] rss = rr.next(FETCH_ROWS); rss != null && rss.length > 0; rss = rr.next(FETCH_ROWS)) {
                for (Result rs : rss) {
                    T retBean = Clazz.newInstance(beanClass);
                    ann.setRowkey(retBean, rs.getRow());
                    setValues(rs.getRow(), retBean, ann, rs, options);
                    arrayList.add(retBean);
                    if (maxRows > 0 && ++rows == maxRows) {
                        break L;
                    }
                }
            }
            return arrayList;
        }
        catch (IOException e) {
            throw new HDaoException(e);
        }
        finally {
            closeHTable(hTable);
        }
    }

    @Override
    public <T> void trunc(Class<T> beanClass) throws HTableDefException {
        HTableBeanAnn ann = HTableBeanAnnMgr.getBeanAnn(hbaesInstanceName, beanClass);
        HBaseAdmin admin = null;
        try {
            admin = new HBaseAdmin(HTablePoolManager.getHBaseConfiguration(hbaesInstanceName));
            String tableName = ann.getHBaseTable().name();
            HTableDescriptor tableDescriptor = admin.getTableDescriptor(Bytes.toBytes(tableName));

            if (!admin.isTableDisabled(tableName)) {
                admin.disableTable(tableName);
            }
            admin.deleteTable(tableName);
            admin.createTable(tableDescriptor);
            //admin.close();
        }
        catch (Exception e) {
            throw new HTableDefException(e);
        }
        finally {
            // closeQuietly(admin);
        }
    }
}
