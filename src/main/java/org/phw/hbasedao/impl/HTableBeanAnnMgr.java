package org.phw.hbasedao.impl;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.phw.hbasedao.annotations.HBaseTable;
import org.phw.hbasedao.annotations.HCascade;
import org.phw.hbasedao.annotations.HColumn;
import org.phw.hbasedao.annotations.HDynamic;
import org.phw.hbasedao.annotations.HParent;
import org.phw.hbasedao.annotations.HRowkey;
import org.phw.hbasedao.annotations.HRowkeyPart;
import org.phw.hbasedao.ex.HTableDefException;
import org.phw.hbasedao.pool.HTablePoolManager;
import org.phw.hbasedao.util.Strs;

import static com.google.common.io.Closeables.*;

public class HTableBeanAnnMgr {
    private static volatile HashMap<Class<?>, HTableBeanAnn> cache = new HashMap<Class<?>, HTableBeanAnn>();

    public static <T> HTableBeanAnn getBeanAnn(String hbaesInstanceName, Class<T> beanClass) throws HTableDefException {
        return getBeanAnn(hbaesInstanceName, null, beanClass);
    }

    public static <T, P> HTableBeanAnn getBeanAnn(String hbaesInstanceName, Class<P> parentClass, Class<T> beanClass)
            throws HTableDefException {
        HTableBeanAnn hTableBeanAnn = cache.get(beanClass);
        if (hTableBeanAnn != null) {
            return hTableBeanAnn;
        }

        synchronized (cache) {
            // 检查对象是否使用HTable标注
            HBaseTable htableAnn = beanClass.getAnnotation(HBaseTable.class);
            if (htableAnn == null) {
                throw new HTableDefException(beanClass + " is not annotationed by HTable");
            }

            // 检查表是否已经创建
            checkTableExistence(hbaesInstanceName, htableAnn, beanClass);

            hTableBeanAnn = new HTableBeanAnn();
            hTableBeanAnn.setHBaseTable(beanClass);

            // 检查HRowkey, HColumn, HDynamicColumn标签
            for (Field field : beanClass.getDeclaredFields()) {
                processHRowkey(hTableBeanAnn, field);
                processHRowkeyPart(hTableBeanAnn, field);
                processHColumn(hTableBeanAnn, field);
                processHDynamic(hTableBeanAnn, field);
                processHCascade(hbaesInstanceName, beanClass, hTableBeanAnn, field);
                processHParent(parentClass, hTableBeanAnn, field);
            }

            checkAnnotion(beanClass, hTableBeanAnn);
            hTableBeanAnn.afterPropertiesSet();
            cache.put(beanClass, hTableBeanAnn);
        }

        return hTableBeanAnn;

    }

    private static <T> void processHParent(Class<T> parentClass, HTableBeanAnn hTableBeanAnn, Field field)
            throws HTableDefException {
        HParent hParent = field.getAnnotation(HParent.class);
        if (hParent == null) {
            return;
        }

        if (hTableBeanAnn.getHParentField() != null) {
            throw new HTableDefException("@HParent can only define on no more than one field.");
        }

        if (parentClass != null && field.getType() != parentClass) {
            throw new HTableDefException("@HParent can only define on the field whose type is same with its parent.");
        }

        hTableBeanAnn.setHParent(field);
    }

    private static <T> void processHCascade(String hbaesInstanceName, Class<T> beanClass, HTableBeanAnn hTableBeanAnn,
            Field field)
            throws HTableDefException {
        HCascade hRelateTo = field.getAnnotation(HCascade.class);
        if (hRelateTo == null) {
            return;
        }

        Class<?> clazz = getCascadeClass(field);
        if (clazz == void.class) {
            throw new HTableDefException("@Cascade cannot detect its clazz property.");
        }
        getBeanAnn(hbaesInstanceName, beanClass, clazz);
        hTableBeanAnn.addHRelateTo(field);
    }

    public static Class<?> getCascadeClass(Field field) {
        HCascade hRelateTo = field.getAnnotation(HCascade.class);
        Class<?> clazz = hRelateTo.clazz();
        if (clazz == void.class) {
            if (List.class.isAssignableFrom(field.getType())
                    && ParameterizedType.class.isAssignableFrom(field.getGenericType().getClass())) {
                ParameterizedType genericType = (ParameterizedType) field.getGenericType();
                clazz = (Class<?>) genericType.getActualTypeArguments()[0];
            }
            else {
                clazz = field.getType();
            }
        }

        return clazz;
    }

    private static void checkAnnotion(Class<?> beanClass, HTableBeanAnn hTableBeanAnn) throws HTableDefException {
        if (hTableBeanAnn.getHRowkeyField() == null && hTableBeanAnn.getHRowkeyPartFields().size() == 0) {
            throw new HTableDefException(beanClass + " does not define a @HRowkey or @HRowkeyPart field.");
        }
        if (hTableBeanAnn.getHColumnFields().size() == 0 && hTableBeanAnn.getHDynamicFields().size() == 0) {
            throw new HTableDefException(beanClass + " should define at least one @HColumn or @HDynamic filed.");
        }
    }

    private static void processHDynamic(HTableBeanAnn hTableBeanAnn, Field field) throws HTableDefException {
        HDynamic hdynamic = field.getAnnotation(HDynamic.class);
        if (hdynamic == null) {
            return;
        }

        // 检查rowkey的字段类型是否是TypeConvertable
        if (!Map.class.isAssignableFrom(field.getType())) {
            throw new HTableDefException("@HDynamic can only defined on Map object");
        }

        HBaseTable hbaseTable = (HBaseTable) hTableBeanAnn.getBeanClass().getAnnotation(HBaseTable.class);
        checkFamily(field.getName(), hdynamic.family(), hbaseTable.families());
        hTableBeanAnn.addHDynamic(field);
    }

    private static void processHColumn(HTableBeanAnn hTableBeanAnn, Field field) throws HTableDefException {
        HColumn hcolumn = field.getAnnotation(HColumn.class);
        if (hcolumn == null) {
            return;
        }

        HBaseTable hbaseTable = (HBaseTable) hTableBeanAnn.getBeanClass().getAnnotation(HBaseTable.class);
        checkFamily(field.getName(), hcolumn.family(), hbaseTable.families());
        hTableBeanAnn.addHColumn(field);
    }

    private static void checkFamily(String fieldName, String family, String[] families) throws HTableDefException {
        if (Strs.isEmpty(family) && (families == null || families.length == 0 || Strs.isEmpty(families[0]))) {
            throw new HTableDefException(fieldName + " does not define a family");
        }
    }

    private static void processHRowkey(HTableBeanAnn hTableBeanAnn, Field field) throws HTableDefException {
        HRowkey hrowkey = field.getAnnotation(HRowkey.class);
        if (hrowkey == null) {
            return;
        }

        if (hTableBeanAnn.getHRowkeyField() != null) {
            throw new HTableDefException("@HRowkey can only define on no more than one field.");
        }
        if (hTableBeanAnn.getHRowkeyPartFields().size() > 0) {
            throw new HTableDefException("@HRowkey can not defined along with @HRowkeyPart.");
        }

        hTableBeanAnn.setHRowkey(field);
    }

    private static void processHRowkeyPart(HTableBeanAnn hTableBeanAnn, Field field) throws HTableDefException {
        HRowkeyPart hRowkeyPart = field.getAnnotation(HRowkeyPart.class);
        if (hRowkeyPart != null) {
            hTableBeanAnn.addHRowkeyPart(field);
        }
        if (hTableBeanAnn.getHRowkeyPartFields().size() > 0 && hTableBeanAnn.getHRowkeyField() != null) {
            throw new HTableDefException("@HRowkey can not defined along with @HRowkeyPart.");
        }
    }

    private static void checkTableExistence(String hbaesInstanceName, HBaseTable htableAnn, Class<?> beanClass)
            throws HTableDefException {
        // 检查HTable表名是否定义
        String tableName = htableAnn.name();
        if (Strs.isEmpty(tableName)) {
            throw new HTableDefException(beanClass + " is annotationed by @HTable with empty name");
        }

        HBaseAdmin admin = null;
        try {
            admin = new HBaseAdmin(HTablePoolManager.getHBaseConfiguration(hbaesInstanceName));
            if (!admin.tableExists(tableName)) {
                if (!htableAnn.autoCreate()) {
                    throw new HTableDefException(tableName + " does not exist");
                }
                if (htableAnn.families() == null || htableAnn.families().length == 0) {
                    throw new HTableDefException(tableName + " does not define its families");
                }

                HTableDescriptor tableDesc = new HTableDescriptor(tableName);
                for (String fam : htableAnn.families()) {
                    tableDesc.addFamily(new HColumnDescriptor(fam));
                }
                admin.createTable(tableDesc);
            }
            else if (!admin.isTableEnabled(tableName)) {
                throw new HTableDefException(tableName + " is not enabled");
            }
            admin.close();
        }
        catch (Exception e) {
            throw new HTableDefException(e);
        }
        finally {
            closeQuietly(admin);
        }
    }
}
