package org.phw.hbasedao.twofamilies;

import junit.framework.Assert;

import org.junit.Test;
import org.phw.hbasedao.DefaultHDao;
import org.phw.hbasedao.HDao;
import org.phw.hbasedao.ex.HDaoException;

public class BeanOneTest {
    @Test
    public void test() throws HDaoException {
        BeanOne beanOne = new BeanOne();
        beanOne.setId("1");
        beanOne.setName("xxx");
        beanOne.setSign(true);
        beanOne.setTimestamp(32033);
        HDao hdao = new DefaultHDao();
        hdao.put(beanOne);

        BeanOne beanOne2 = hdao.get(BeanOne.class, "1");
        Assert.assertEquals(beanOne, beanOne2);

        // 只查f列族的内容，其它列族的内容不会返回
        BeanOne beanOne3 = hdao.get(BeanOne.class, "1", "f");
        Assert.assertFalse(beanOne3.isSign());
        Assert.assertEquals(0, beanOne3.getTimestamp());

        beanOne3 = hdao.get(BeanOne.class, "1", "f", "a");
        Assert.assertFalse(beanOne3.isSign());
        Assert.assertEquals(32033, beanOne3.getTimestamp());
    }
}
