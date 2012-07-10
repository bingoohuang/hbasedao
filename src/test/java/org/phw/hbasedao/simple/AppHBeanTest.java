package org.phw.hbasedao.simple;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.phw.hbasedao.DefaultHDao;
import org.phw.hbasedao.HDao;
import org.phw.hbasedao.ex.HDaoException;
import org.phw.hbasedao.pool.HTablePoolManager;

import com.google.common.collect.Lists;

public class AppHBeanTest {
    @Test
    public void test1() throws HDaoException {
        HTablePoolManager.createHBaseConfiguration("bjtest", "10.142.151.88", "2181");
        AppHBean appHBean = new AppHBean();
        appHBean.setRowkey(1L);

        HDao hdao = new DefaultHDao("bjtest");
        hdao.delete(AppHBean.class, 1L);

        List<String> colcontent = Lists.newArrayList("a", "b", "C");
        appHBean.setColcontent(colcontent);
        hdao.put(appHBean);

        AppHBean appHBean2 = hdao.get(AppHBean.class, 1L);
        Assert.assertEquals(colcontent, appHBean2.getColcontent());

    }
}
