package org.phw.hbasedao.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import junit.framework.Assert;

import org.apache.hadoop.io.IOUtils;
import org.junit.Test;
import org.phw.hbasedao.DefaultHDao;
import org.phw.hbasedao.HDao;
import org.phw.hbasedao.ex.HDaoException;
import org.phw.hbasedao.pool.HTablePoolManager;

public class AnnDemoBeanTest {
    @Test
    public void testBase() throws HDaoException, IOException {
        File f = new File("src/test/resources/anndemo.jpg");
        byte[] img = new byte[(int) f.length()];

        InputStream is = new FileInputStream(f);
        is.read(img);
        IOUtils.closeStream(is);
        AnnDemoBean adb = new AnnDemoBean();
        adb.setRowkey("T00000");
        adb.setCol1(0);
        adb.setCol2('A');
        adb.setCol3(img);

        HDao hdao = new DefaultHDao();
        hdao.trunc(AnnDemoBean.class);
        boolean insert = hdao.insert(adb);
        Assert.assertTrue(insert);

        AnnDemoBean temp = hdao.get(AnnDemoBean.class, "T00000");
        Assert.assertEquals(img.length, temp.getCol3().length);

        Assert.assertNotNull(HTableBeanAnnMgr.getBeanAnn(HTablePoolManager.DEFAULT_INSTANCE, AnnDemoBean.class));
    }
}
