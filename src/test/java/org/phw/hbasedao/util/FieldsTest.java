package org.phw.hbasedao.util;

import java.lang.reflect.Field;

import junit.framework.Assert;

import org.junit.Test;
import org.phw.hbasedao.ex.HTableDefException;

import com.esotericsoftware.reflectasm.FieldAccess;
import com.esotericsoftware.reflectasm.MethodAccess;

public class FieldsTest {
    @Test
    public void testBase() {
        SampleBean bean = new SampleBean();
        Field f = null;
        try {
            f = Fields.getDeclaredField(SampleBean.class, "name");
        }
        catch (HTableDefException e1) {
        }
        try {
            Assert.assertEquals(SampleBean.class.getDeclaredField("name"), f);
        }
        catch (SecurityException e) {
        }
        catch (NoSuchFieldException e) {
        }
        bean.setName("whc");
        Assert.assertEquals("whc",
                Fields.getFieldValue(MethodAccess.get(SampleBean.class), FieldAccess.get(SampleBean.class), bean, f));
        Fields.setFieldValue(MethodAccess.get(SampleBean.class), FieldAccess.get(SampleBean.class), bean, f, "else");
        Assert.assertEquals("else", bean.getName());
    }
}
