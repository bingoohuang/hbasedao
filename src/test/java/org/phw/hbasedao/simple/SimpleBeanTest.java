package org.phw.hbasedao.simple;

import java.util.List;

import junit.framework.Assert;

import org.junit.Test;
import org.phw.hbasedao.DefaultHDao;
import org.phw.hbasedao.HDao;
import org.phw.hbasedao.ex.HDaoException;
import org.phw.hbasedao.pool.HTablePoolManager;

public class SimpleBeanTest {
    @Test
    public void testPutAndGet() throws HDaoException {
        //HTablePoolManager.createHBaseConfiguration("bjtest", "10.142.195.67,10.142.151.88,10.142.195.63", "2181");
        HTablePoolManager.getHTablePool("bjtest", "10.20.16.32,10.20.16.35,10.20.16.36", "2181");
        SimpleBean simpleBean1 = new SimpleBean();
        simpleBean1.setRowkey("H40685");
        simpleBean1.setName("黄进兵");
        simpleBean1.setAdult(true);
        simpleBean1.setAge(33);

        HDao hdao = new DefaultHDao("bjtest");
        hdao.trunc(SimpleBean.class); // 会删除并且重建表，表所有数据丢失。危险，慎用

        boolean insert = hdao.insert(simpleBean1);
        Assert.assertTrue(insert);

        insert = hdao.insert(simpleBean1);
        Assert.assertFalse(insert);
        simpleBean1.setRowkey("H40684");
        boolean update = hdao.update(simpleBean1);
        Assert.assertFalse(update);

        simpleBean1.setRowkey("H40685");
        simpleBean1.setAge(34);
        update = hdao.update(simpleBean1);
        Assert.assertTrue(update);

        SimpleBean simpleBean2 = hdao.get(SimpleBean.class, "H40684");
        Assert.assertNull(simpleBean2);

        simpleBean2 = hdao.get(SimpleBean.class, "H40685");
        Assert.assertEquals(simpleBean1, simpleBean2);

        simpleBean2 = hdao.get(SimpleBean.class, "H40685", "f");
        Assert.assertEquals(simpleBean1, simpleBean2);

        simpleBean2 = hdao.get(SimpleBean.class, "H40684", "f");
        Assert.assertNull(simpleBean2);

        hdao.put(SimpleBean.class, simpleBean1, "other", "thing", "some", "object");
        String string1 = hdao.get(SimpleBean.class, "H40685", String.class, "other");
        Assert.assertEquals("thing", string1);
        String string2 = hdao.get(SimpleBean.class, "H40685", String.class, "some");
        Assert.assertEquals("object", string2);
        hdao.delete(SimpleBean.class, "H40685", "other", "some");
        string2 = hdao.get(SimpleBean.class, "H40685", String.class, "some");
        Assert.assertNull("object", string2);

        simpleBean2 = new SimpleBean();
        simpleBean2.setRowkey("H40686");
        simpleBean2.setName("黄进兵2");
        simpleBean2.setAdult(false);
        simpleBean2.setAge(35);
        hdao.put(simpleBean2);

        List<SimpleBean> list = hdao.query(SimpleBean.class, "H40685", "H40687");
        Assert.assertEquals(2, list.size());
        Assert.assertEquals(simpleBean1, list.get(0));
        Assert.assertEquals(simpleBean2, list.get(1));
        list = hdao.query(SimpleBean.class, "H40685", null);
        Assert.assertEquals(2, list.size());
        Assert.assertEquals(simpleBean1, list.get(0));
        Assert.assertEquals(simpleBean2, list.get(1));

        //hdao.delete(SimpleBean.class, "H40685");
        //hdao.delete(SimpleBean.class, "H40686");
    }
}
