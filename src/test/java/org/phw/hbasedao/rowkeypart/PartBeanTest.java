package org.phw.hbasedao.rowkeypart;

import junit.framework.Assert;

import org.junit.Test;
import org.phw.hbasedao.DefaultHDao;
import org.phw.hbasedao.HDao;
import org.phw.hbasedao.ex.HDaoException;

public class PartBeanTest {
    @Test
    public void test1() throws HDaoException {
        PartBean partBean = new PartBean();
        partBean.setCode("XXYY");
        partBean.setId(1);
        partBean.setValue("hello");

        HDao hDao = new DefaultHDao();
        hDao.put(partBean);

        PartBean partBean2 = new PartBean();
        partBean2.setCode("XXYY");
        partBean2.setId(1);
        PartBean partBean3 = hDao.get(PartBean.class, partBean2);

        Assert.assertEquals(partBean, partBean3);
    }
}
