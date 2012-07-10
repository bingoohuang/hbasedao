package org.phw.hbasedao;

import java.util.EnumSet;
import java.util.List;

import org.phw.hbasedao.ex.HDaoException;
import org.phw.hbasedao.ex.HTableDefException;

public interface HDao {
    // --- put/get/insert/update POJO --- 
    /**
     * 插入或者更新。
     * @param bean
     * @throws HDaoException
     */
    <T> void put(T bean) throws HDaoException;

    /**
     * 批量插入或者批量更新
     * @param beans
     * @throws HDaoException
     */
    <T> void put(List<T> beans) throws HDaoException;

    /**
     * 插入一行数据。
     * @param bean
     * @return true 插入成功 false 行已经存在
     * @throws HDaoException
     */
    <T> boolean insert(T bean) throws HDaoException;

    /**
     * 更新一行数据。
     * @param bean
     * @return true 更新成功 false 行不存在
     * @throws HDaoException
     */
    <T> boolean update(T bean) throws HDaoException;

    /**
     * 插入或者更新。
     * @param bean
     * @throws HDaoException
     */
    <T> void put(T bean, EnumSet<DaoOption> options) throws HDaoException;

    /**
     * 批量插入或者更新。
     * @param bean
     * @throws HDaoException
     */
    <T> void put(List<T> beans, EnumSet<DaoOption> options) throws HDaoException;

    /**
     * 插入一行数据。
     * @param bean
     * @return true 插入成功 false 行已经存在
     * @throws HDaoException
     */
    <T> boolean insert(T bean, EnumSet<DaoOption> options) throws HDaoException;

    /**
     * 更新一行数据。
     * @param bean
     * @return true 更新成功 false 行不存在
     * @throws HDaoException
     */
    <T> boolean update(T bean, EnumSet<DaoOption> options) throws HDaoException;

    /**
     * 根据rowkey范围查询。
     * @param beanClass
     * @param startRowkey
     * @param stopRowkey
     * @param maxRows MAX ROWS TO RETURN. 小于等于0表示不限制。
     * @return
     * @throws HDaoException
     */
    <T> List<T> query(Class<T> beanClass, Object startRowkey, Object stopRowkey) throws HDaoException;

    <T> List<T> query(Class<T> beanClass, Object startRowkey, Object stopRowkey, int maxRows) throws HDaoException;

    <T> List<T> query(Class<T> beanClass, Object startRowkey, Object stopRowkey, int maxRows, EnumSet<DaoOption> options)
            throws HDaoException;

    /**
     * 获取数据。
     * @param bean
     * @return
     * @throws HDaoException
     */
    <T> T get(Class<T> beanClass, Object rowkey, EnumSet<DaoOption> options) throws HDaoException;

    /**
     * 获取数据。
     * @param bean
     * @return
     * @throws HDaoException
     */
    <T> T get(Class<T> beanClass, Object rowkey) throws HDaoException;

    /**
     * 只获取指定列族的数据(rowkey单独指定)。
     * @param beanClass
     * @param rowkey
     * @param family
     * @param families
     * @return
     * @throws HDaoException
     */
    <T> T get(Class<T> beanClass, Object rowkey, String family, String... families) throws HDaoException;

    /**
     * 获取默认列族的单个列取值。
     * @param beanClass
     * @param rowkey
     * @param valueType
     * @param key
     * @return
     * @throws HDaoException
     */
    <T, V> V get(Class<T> beanClass, Object rowkey, Class<V> valueType, Object key) throws HDaoException;

    /**
     * 获取单个列取值.
     * @param family
     * @param bean
     * @param key
     * @return
     * @throws HDaoException
     */
    <T, V> V get(String family, Class<T> beanClass, Object rowkey, Class<V> valueType, Object key) throws HDaoException;

    /**
     * 单独添加默认列族的key vlaues.
     * @param beanClass
     * @param rowkey
     * @param key
     * @param value
     * @param kvs
     * @throws HDaoException
     */
    <T> void put(Class<T> beanClass, Object rowkey, Object key, Object value, Object... kvs) throws HDaoException;

    /**
     * 单独添加列族的key vlaues.
     * @param family
     * @param beanClass
     * @param rowkey
     * @param key
     * @param value
     * @param kvs
     * @throws HDaoException
     */
    <T> void put(String family, Class<T> beanClass, Object rowkey, Object key, Object value, Object... kvs)
            throws HDaoException;

    /**
     * 锁定行（rowkey单独指定）。
     * @param beanClass
     * @param rowkey
     * @return
     * @throws HDaoException
     */
    <T> DaoRowLock lockRow(Class<T> beanClass, Object rowkey) throws HDaoException;

    /**
     * 释放行锁。
     * @param daoRowlock
     * @throws HDaoException
     */
    void unlockRow(DaoRowLock daoRowlock) throws HDaoException;

    /**
     * 删除整行。
     * @param bean
     * @return
     * @throws HDaoException
     */
    <T> void delete(Class<T> beanClass, Object rowkey) throws HDaoException;

    <T> void delete(EnumSet<DaoOption> options, Class<T> beanClass, Object rowkey) throws HDaoException;

    /**
     * 删除Keys.
     * @param family
     * @param bean
     * @param keys
     * @throws HDaoException
     */
    <T> void delete(String family, Class<T> beanClass, Object rowkey, Object key, Object... keys) throws HDaoException;

    /**
     * 删除默认列族所在Keys.
     * @param bean
     * @param keys
     * @throws HDaoException
     */
    <T> void delete(Class<T> beanClass, Object rowkey, Object key, Object... keys) throws HDaoException;

    /**
     * 清除表数据.
     * 危险，请慎用！！！！
     * @param beanClass
     * @throws HTableDefException 
     */
    <T> void trunc(Class<T> beanClass) throws HTableDefException;

}
