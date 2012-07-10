package org.phw.hbasedao;

import java.util.EnumSet;
import java.util.List;

import org.phw.hbasedao.ex.HDaoException;

public abstract class BaseHDao implements HDao {
    @Override
    public <T> void put(T bean) throws HDaoException {
        put(bean, EnumSet.noneOf(DaoOption.class));
    }

    @Override
    public <T> void put(List<T> beans) throws HDaoException {
        put(beans, EnumSet.noneOf(DaoOption.class));
    }

    @Override
    public <T> void delete(Class<T> beanClass, Object rowkey) throws HDaoException {
        delete(EnumSet.noneOf(DaoOption.class), beanClass, rowkey);
    }

    @Override
    public <T, V> V get(Class<T> beanClass, Object rowkey, Class<V> valueType, Object key) throws HDaoException {
        return get("", beanClass, rowkey, valueType, key);
    }

    @Override
    public <T> void put(Class<T> beanClass, Object rowkey, Object key, Object value, Object... kvs)
            throws HDaoException {
        put("", beanClass, rowkey, key, value, kvs);
    }

    @Override
    public <T> List<T> query(Class<T> beanClass, Object startRowkey, Object stopRowkey) throws HDaoException {
        return query(beanClass, startRowkey, stopRowkey, 0);
    }

    @Override
    public <T> void delete(Class<T> beanClass, Object rowkey, Object key, Object... keys) throws HDaoException {
        delete("", beanClass, rowkey, key, keys);
    }

    @Override
    public <T> boolean insert(T bean) throws HDaoException {
        return merge(bean, true, EnumSet.noneOf(DaoOption.class));
    }

    @Override
    public <T> boolean update(T bean) throws HDaoException {
        return merge(bean, false, EnumSet.noneOf(DaoOption.class));
    }

    @Override
    public <T> boolean insert(T bean, EnumSet<DaoOption> options) throws HDaoException {
        return merge(bean, true, options);
    }

    @Override
    public <T> boolean update(T bean, EnumSet<DaoOption> options) throws HDaoException {
        return merge(bean, false, options);
    }

    @Override
    public <T> T get(Class<T> beanClass, Object rowkey) throws HDaoException {
        return getImpl(beanClass, EnumSet.noneOf(DaoOption.class), rowkey);
    }

    @Override
    public <T> T get(Class<T> beanClass, Object rowkey, EnumSet<DaoOption> options) throws HDaoException {
        return getImpl(beanClass, options, rowkey);
    }

    @Override
    public <T> List<T> query(Class<T> beanClass, Object startRowkey, Object stopRowkey, int maxRows)
            throws HDaoException {
        return queryImpl(beanClass, startRowkey, stopRowkey, maxRows, EnumSet.noneOf(DaoOption.class));
    }

    @Override
    public <T> List<T> query(Class<T> beanClass, Object startRowkey, Object stopRowkey, int maxRows,
            EnumSet<DaoOption> options)
            throws HDaoException {
        return queryImpl(beanClass, startRowkey, stopRowkey, maxRows, options);
    }

    abstract protected <T> T getImpl(Class<T> beanClass, EnumSet<DaoOption> options, Object bean) throws HDaoException;

    abstract protected <T> T getImpl(Class<T> beanClass, Object bean, String family, String... families)
            throws HDaoException;

    abstract protected <T> List<T> queryImpl(Class<T> beanClass, Object startRowkey, Object stopRowkey, int maxRows,
            EnumSet<DaoOption> options) throws HDaoException;

    abstract protected <T> boolean merge(T bean, boolean isInsert, EnumSet<DaoOption> options) throws HDaoException;
}
