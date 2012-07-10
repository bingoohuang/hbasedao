package org.phw.hbasedao.cascade;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;
import org.phw.hbasedao.DaoOption;
import org.phw.hbasedao.DefaultHDao;
import org.phw.hbasedao.HDao;
import org.phw.hbasedao.ex.HDaoException;

public class MainBeanTest {
    @Test
    public void test() throws HDaoException {
        MainBean mainBean = new MainBean();
        mainBean.setId("ABCD");
        mainBean.setName("佛教");

        OtherBean otherBean = new OtherBean();
        otherBean.setId("ABCD");
        otherBean.setColor("金色");

        mainBean.setOtherBean(otherBean);

        List<SubItemBean> subItems = new ArrayList<SubItemBean>();
        SubItemBean subItem = new SubItemBean();
        subItems.add(subItem);
        subItem.setId("ABCD");
        subItem.setSeq(0);
        subItem.setItemName("金刚经");

        subItem = new SubItemBean();
        subItems.add(subItem);
        subItem.setId("ABCD");
        subItem.setSeq(1);
        subItem.setItemName("心经");

        subItem = new SubItemBean();
        subItems.add(subItem);
        subItem.setId("ABCD");
        subItem.setSeq(2);
        subItem.setItemName("法华经");

        mainBean.setSubItems(subItems);

        List<SubItem2Bean> subItems2 = new ArrayList<SubItem2Bean>();
        SubItem2Bean subItem2 = new SubItem2Bean();
        subItem2.setId("ABCD");
        subItem2.setSeq(0);
        subItem2.setItemName("如来");
        subItems2.add(subItem2);
        subItem2 = new SubItem2Bean();
        subItem2.setId("ABCD");
        subItem2.setSeq(1);
        subItem2.setItemName("观世音");
        subItems2.add(subItem2);
        mainBean.setSubItems2(subItems2);

        HDao dao = new DefaultHDao();
        dao.put(mainBean, EnumSet.of(DaoOption.CASCADE));

        MainBean mainBean2 = dao.get(MainBean.class, "ABCD", EnumSet.of(DaoOption.CASCADE));
        Assert.assertEquals(mainBean, mainBean2);
        mainBean2 = dao.get(MainBean.class, "ABCD");
        Assert.assertNull(mainBean2.getSubItems());
        Assert.assertNull(mainBean2.getSubItems2());
        Assert.assertNull(mainBean2.getOtherBean());

        dao.delete(EnumSet.of(DaoOption.CASCADE), MainBean.class, "ABCD");
    }
}
