package org.phw.hbasedao.impl;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.phw.hbasedao.annotations.HBaseTable;
import org.phw.hbasedao.annotations.HCascade;
import org.phw.hbasedao.annotations.HColumn;
import org.phw.hbasedao.annotations.HDynamic;
import org.phw.hbasedao.annotations.HParent;
import org.phw.hbasedao.annotations.HRowkey;
import org.phw.hbasedao.annotations.HRowkeyPart;
import org.phw.hbasedao.ex.HTableDefException;
import org.phw.hbasedao.pool.HTablePoolManager;
import org.phw.hbasedao.util.Clazz;
import org.phw.hbasedao.util.Strs;

public class HTableBeanAnnMgr {
    private static volatile HashMap<Class<?>, HTableBeanAnn> cache = new HashMap<Class<?>, HTableBeanAnn>();
    private static volatile HashSet<String> tableExistanceCheckCache = new HashSet<String>();

    public static <T> HTableBeanAnn getBeanAnn(String hbaesInstanceName, Class<T> beanClass) throws HTableDefException {
        return getBeanAnn(hbaesInstanceName, null, beanClass);
    }

    private static <T, P> HTableBeanAnn getBeanAnn(String hbaesInstanceName, Class<P> parentClass, Class<T> beanClass)
            throws HTableDefException {
        HTableBeanAnn hTableBeanAnn = cache.get(beanClass);
        if (hTableBeanAnn != null) return hTableBeanAnn;

        synchronized (cache) {
            hTableBeanAnn = cache.get(beanClass);
            if (hTableBeanAnn != null) return hTableBeanAnn;

            HBaseTable htableAnn = beanClass.getAnnotation(HBaseTable.class);
            if (htableAnn == null) throw new HTableDefException(beanClass + " is not annotationed by HTable");

            checkTableExistence(hbaesInstanceName, htableAnn, beanClass);

            hTableBeanAnn = new HTableBeanAnn();
            hTableBeanAnn.setHBaseTable(beanClass);

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
        if (field.getAnnotation(HParent.class) == null) return;

        if (hTableBeanAnn.getHParentField() != null)
            throw new HTableDefException("@HParent can only define on no more than one field.");

        if (parentClass != null && field.getType() != parentClass)
            throw new HTableDefException("@HParent can only define on the field whose type is same with its parent.");

        hTableBeanAnn.setHParent(field);
    }

    private static <T> void processHCascade(String hbaesInstanceName, Class<T> beanClass, HTableBeanAnn hTableBeanAnn,
            Field field) throws HTableDefException {
        if (field.getAnnotation(HCascade.class) == null) return;

        Class<?> clazz = getCascadeClass(field);
        if (clazz == void.class) throw new HTableDefException("@Cascade cannot detect its clazz property.");

        getBeanAnn(hbaesInstanceName, beanClass, clazz);
        hTableBeanAnn.addHRelateTo(field);
    }

    public static Class<?> getCascadeClass(Field field) {
        Class<?> clazz = field.getAnnotation(HCascade.class).clazz();
        if (clazz == void.class) {
            if (List.class.isAssignableFrom(field.getType())
                    && ParameterizedType.class.isAssignableFrom(field.getGenericType().getClass())) {
                ParameterizedType genericType = (ParameterizedType) field.getGenericType();
                clazz = (Class<?>) genericType.getActualTypeArguments()[0];
            }
            else clazz = field.getType();
        }

        return clazz;
    }

    private static void checkAnnotion(Class<?> beanClass, HTableBeanAnn hTableBeanAnn) throws HTableDefException {
        if (hTableBeanAnn.getHRowkeyField() == null && hTableBeanAnn.getHRowkeyPartFields().size() == 0)
            throw new HTableDefException(beanClass + " does not define a @HRowkey or @HRowkeyPart field.");
        if (hTableBeanAnn.getHColumnFields().size() == 0 && hTableBeanAnn.getHDynamicFields().size() == 0)
            throw new HTableDefException(beanClass + " should define at least one @HColumn or @HDynamic filed.");
    }

    private static void processHDynamic(HTableBeanAnn hTableBeanAnn, Field field) throws HTableDefException {
        HDynamic hdynamic = field.getAnnotation(HDynamic.class);
        if (hdynamic == null) return;

        // 检查rowkey的字段类型是否是TypeConvertable
        if (!Map.class.isAssignableFrom(field.getType()))
            throw new HTableDefException("@HDynamic can only defined on Map object");

        HBaseTable hbaseTable = hTableBeanAnn.getBeanClass().getAnnotation(HBaseTable.class);
        checkFamily(field.getName(), hdynamic.family(), hbaseTable.families());
        hTableBeanAnn.addHDynamic(field);
    }

    private static void processHColumn(HTableBeanAnn hTableBeanAnn, Field field) throws HTableDefException {
        HColumn hcolumn = field.getAnnotation(HColumn.class);
        if (hcolumn == null) return;

        HBaseTable hbaseTable = hTableBeanAnn.getBeanClass().getAnnotation(HBaseTable.class);
        checkFamily(field.getName(), hcolumn.family(), hbaseTable.families());
        hTableBeanAnn.addHColumn(field);
    }

    private static void checkFamily(String fieldName, String family, String[] families) throws HTableDefException {
        if (Strs.isEmpty(family) && (families == null || families.length == 0 || Strs.isEmpty(families[0])))
            throw new HTableDefException(fieldName + " does not define a family");
    }

    /**
     * @param hTableBeanAnn
     * @param field
     * @throws HTableDefException
     */
    private static void processHRowkey(HTableBeanAnn hTableBeanAnn, Field field) throws HTableDefException {
        HRowkey hrowkey = field.getAnnotation(HRowkey.class);
        if (hrowkey == null) return;

        if (hTableBeanAnn.getHRowkeyField() != null)
            throw new HTableDefException("@HRowkey can only define on no more than one field.");
        if (hTableBeanAnn.getHRowkeyPartFields().size() > 0)
            throw new HTableDefException("@HRowkey can not defined along with @HRowkeyPart.");

        hTableBeanAnn.setHRowkey(field);
    }

    private static void processHRowkeyPart(HTableBeanAnn hTableBeanAnn, Field field) throws HTableDefException {
        HRowkeyPart hRowkeyPart = field.getAnnotation(HRowkeyPart.class);
        if (hRowkeyPart != null) hTableBeanAnn.addHRowkeyPart(field);

        if (hTableBeanAnn.getHRowkeyPartFields().size() > 0 && hTableBeanAnn.getHRowkeyField() != null)
            throw new HTableDefException("@HRowkey can not defined along with @HRowkeyPart.");
    }

    private static void checkTableExistence(String hbaesInstanceName, HBaseTable htableAnn, Class<?> beanClass)
            throws HTableDefException {
        if (htableAnn.nameCreator() != Void.class) return;

        // 检查HTable表名是否定义
        String tableName = htableAnn.name();
        if (Strs.isEmpty(tableName))
            throw new HTableDefException(beanClass + " is annotationed by @HTable with empty name");

        checkAndCreateTable(hbaesInstanceName, htableAnn, beanClass, tableName);
    }

    protected static void checkAndCreateTable(String hbaesInstanceName, HBaseTable htableAnn, Class<?> beanClass,
            String tableName) throws HTableDefException {
        String cachedName = hbaesInstanceName + "$" + tableName;
        if (tableExistanceCheckCache.contains(cachedName)) return;

        synchronized (tableExistanceCheckCache) {
            if (tableExistanceCheckCache.contains(cachedName)) return;

            checkAndCreateTableWoCache(hbaesInstanceName, htableAnn, tableName);
        }
    }

    protected static void checkAndCreateTableWoCache(String hbaesInstanceName, HBaseTable htableAnn, String tableName)
            throws HTableDefException {
        HBaseAdmin admin = null;
        try {
            admin = new HBaseAdmin(HTablePoolManager.getHBaseConfig(hbaesInstanceName));
            if (!admin.tableExists(tableName)) {
                if (!htableAnn.autoCreate()) throw new HTableDefException(tableName + " does not exist");
                if (htableAnn.families() == null || htableAnn.families().length == 0)
                    throw new HTableDefException(tableName + " does not define its families");

                HTableDescriptor tableDesc = new HTableDescriptor(tableName);
                for (String fam : htableAnn.families())
                    tableDesc.addFamily(new HColumnDescriptor(fam));

                admin.createTable(tableDesc);
            }
            else if (!admin.isTableEnabled(tableName)) throw new HTableDefException(tableName + " is not enabled");

            tableExistanceCheckCache.add(tableName);
            //sadmin.getConnection().
        } catch (Exception e) {
            throw new HTableDefException(e);
        } finally {
            if (admin != null)
                HConnectionManager.deleteConnection(admin.getConfiguration(), true);
            //closeQuietly(admin);
        }
    }

    public static String getTableName(String hbaesInstanceName, HBaseTable hbaseTable, Class<?> beanClass)
            throws HTableDefException {
        if (hbaseTable.nameCreator() == Void.class) return hbaseTable.name();

        Object nameCreator = Clazz.newInstance(hbaseTable.nameCreator());
        Method method = findProperMethod(nameCreator.getClass());

        if (method == null) throw new HTableDefException("no proper method found for " + hbaseTable.nameCreator());

        String tableName = invokeMethod(method, nameCreator, hbaseTable.name());
        if (Strs.isEmpty(tableName))
            throw new HTableDefException(hbaseTable.nameCreator() + " create an empty name");

        if (hbaseTable.autoCreate()) checkAndCreateTable(hbaesInstanceName, hbaseTable, beanClass, tableName);

        return tableName;
    }

    public static Method findProperMethod(Class<?> clazz) {
        Method[] methods = clazz.getMethods();
        for (Method method : methods) {
            if (Modifier.isStatic(method.getModifiers())) continue;
            Class<?>[] parameterTypes = method.getParameterTypes();
            if (parameterTypes.length != 1 || parameterTypes[0] != String.class) continue;
            if (method.getReturnType() != String.class) continue;

            return method;
        }
        return null;
    }

    public static String invokeMethod(Method method, Object nameCreator, String name) throws HTableDefException {
        try {
            return (String) method.invoke(nameCreator, new Object[] { name });
        } catch (Exception e) {
            throw new HTableDefException(e);
        }
    }
}
