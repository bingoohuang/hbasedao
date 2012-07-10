package org.phw.hbasedao.complex2;

import org.junit.Assert;
import org.junit.Test;
import org.phw.hbasedao.DaoRowLock;
import org.phw.hbasedao.DefaultHDao;
import org.phw.hbasedao.HDao;
import org.phw.hbasedao.ex.HDaoException;

public class EopAppBeanTest {
    /**
     * 测试写入和行读取。
     * @throws HDaoException
     */
    @Test
    public void testPutGet() throws HDaoException {
        EopAppBean eopAppBean = new EopAppBean();
        EopAppBeanRowkey rowkey = new EopAppBeanRowkey();
        rowkey.setAppcode("bingoohuang");
        long timestamp = System.currentTimeMillis();
        rowkey.setTimestamp(timestamp);
        eopAppBean.setRowkey(rowkey);
        eopAppBean.setAppid(System.currentTimeMillis());

        HDao dao = new DefaultHDao();
        dao.put(eopAppBean);

        EopAppBean bean2 = dao.get(EopAppBean.class, rowkey);

        Assert.assertEquals(eopAppBean.getAppid(), bean2.getAppid());

        dao.delete(EopAppBean.class, rowkey);
        bean2 = dao.get(EopAppBean.class, rowkey);
        Assert.assertNull(bean2);
    }

    /**
     * 测试删除整行。
     * @throws HDaoException
     */
    @Test
    public void testDeleteRow() throws HDaoException {
        EopAppBean bean = new EopAppBean();
        long timestamp = System.currentTimeMillis();
        EopAppBeanRowkey rowkey = new EopAppBeanRowkey();
        rowkey.setAppcode("bingoohuang");
        rowkey.setTimestamp(timestamp);
        bean.setRowkey(rowkey);
        bean.setAppid(System.currentTimeMillis());

        HDao dao = new DefaultHDao();

        dao.insert(bean);
        EopAppBean bean2 = dao.get(EopAppBean.class, rowkey);
        Assert.assertNotNull(bean2);

        dao.delete(EopAppBean.class, rowkey);

        bean2 = dao.get(EopAppBean.class, rowkey);
        Assert.assertNull(bean2);
    }

    /**
     * 测试删除列.
     * @throws HDaoException
     */
    @Test
    public void testDeleteKeyValues() throws HDaoException {
        EopAppBean bean = new EopAppBean();
        long timestamp = System.currentTimeMillis();
        EopAppBeanRowkey rowkey = new EopAppBeanRowkey();
        rowkey.setAppcode("bingoohuang");
        rowkey.setTimestamp(timestamp);
        bean.setRowkey(rowkey);
        bean.getDynamicProperties().put("key1", "value1");
        bean.getDynamicProperties().put("key2", "value2");

        HDao dao = new DefaultHDao();
        dao.insert(bean);

        EopAppBean bean3 = dao.get(EopAppBean.class, rowkey);
        Assert.assertEquals(bean.getDynamicProperties(), bean3.getDynamicProperties());

        dao.delete(EopAppBean.class, rowkey, "key1");
        bean3 = dao.get(EopAppBean.class, rowkey);
        Assert.assertNull(bean3.getDynamicProperties().get("key1"));

        dao.delete(EopAppBean.class, rowkey);
    }

    /**
     * 测试行锁定和解锁。
     * @throws HDaoException
     */
    @Test
    public void testLockUnlock() throws HDaoException {
        EopAppBean eopAppBean = new EopAppBean();
        long timestamp = System.currentTimeMillis();
        EopAppBeanRowkey rowkey = new EopAppBeanRowkey();
        rowkey.setAppcode("bingoohuang");
        rowkey.setTimestamp(timestamp);
        eopAppBean.setRowkey(rowkey);
        eopAppBean.setAppid(System.currentTimeMillis());

        HDao dao = new DefaultHDao();
        DaoRowLock rowlock = null;
        try {
            rowlock = dao.lockRow(EopAppBean.class, rowkey);
        }
        finally {
            dao.unlockRow(rowlock);
        }
    }

    /**
     * 测试插入行和更新行
     * @throws HDaoException
     */
    @Test
    public void testInsertUpdate() throws HDaoException {
        EopAppBean eopAppBean = new EopAppBean();
        EopAppBeanRowkey rowkey = new EopAppBeanRowkey();
        rowkey.setAppcode("bingoohuang");
        rowkey.setTimestamp(System.currentTimeMillis());
        eopAppBean.setRowkey(rowkey);
        eopAppBean.setAppid(System.currentTimeMillis());

        HDao dao = new DefaultHDao();
        boolean ok = dao.update(eopAppBean);
        Assert.assertFalse(ok);

        ok = dao.insert(eopAppBean);
        Assert.assertTrue(ok);
        ok = dao.insert(eopAppBean);
        Assert.assertFalse(ok);

        eopAppBean.setAppid(System.currentTimeMillis() + 999);
        ok = dao.update(eopAppBean);
        Assert.assertTrue(ok);

        dao.delete(EopAppBean.class, rowkey);
    }
}
