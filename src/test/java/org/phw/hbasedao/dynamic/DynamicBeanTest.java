package org.phw.hbasedao.dynamic;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;
import org.phw.hbasedao.DefaultHDao;
import org.phw.hbasedao.HDao;
import org.phw.hbasedao.ex.HDaoException;

public class DynamicBeanTest {
    @Test
    public void test() throws HDaoException {
        DynamicBean dynamicBean = new DynamicBean();
        dynamicBean.setRowkey("dynamictest001");
        dynamicBean.setAppid(30000);
        dynamicBean.setDesc("It's bingoohuang's dynamic columns test001");

        Map<Object, Object> dynamicProperties = new HashMap<Object, Object>();

        SignKey signKey = new SignKey();
        signKey.setEff(100);
        signKey.setExp(2000);

        dynamicProperties.put(signKey, "bingoohuang's signkey");

        ParamKey paramKey = new ParamKey();
        paramKey.setEff(111111);
        paramKey.setExp(777777);
        ParamKeyValue paramKeyValue = new ParamKeyValue();
        paramKeyValue.setSalt(333);
        paramKeyValue.setSecurity("paramsecurity");
        dynamicProperties.put(paramKey, paramKeyValue);

        dynamicProperties.put("what", "this");

        dynamicBean.setDynamicProperties(dynamicProperties);

        HDao dao = new DefaultHDao();
        boolean ok = dao.insert(dynamicBean);
        Assert.assertTrue(ok);

        DynamicBean dynamicBean2 = dao.get(DynamicBean.class, "dynamictest001");
        Assert.assertEquals(dynamicBean, dynamicBean2);

        dao.delete(DynamicBean.class, "dynamictest001");
    }
}
